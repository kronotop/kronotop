/*
 * Copyright (c) 2023 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.core.cluster.sharding;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.core.cluster.coordinator.events.TaskCompletedEvent;
import com.kronotop.core.cluster.coordinator.tasks.AssignShardTask;
import com.kronotop.core.cluster.coordinator.tasks.BaseTask;
import com.kronotop.core.cluster.coordinator.tasks.ReassignShardTask;
import com.kronotop.core.cluster.coordinator.tasks.TaskType;
import com.kronotop.core.cluster.journal.Event;
import com.kronotop.core.cluster.journal.Journal;
import com.kronotop.core.cluster.journal.JournalMetadata;
import com.kronotop.core.cluster.journal.JournalName;
import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.storage.impl.OnHeapShardImpl;
import com.kronotop.redis.storage.persistence.DataStructure;
import com.kronotop.redis.storage.persistence.ShardLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ShardingService implements KronotopService {
    public static final String NAME = "Sharding";
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardingService.class);
    private final Context context;
    private final Journal journal;
    private final AtomicLong lastOffset;
    private final HashMap<Integer, DirectorySubspace> shardsSubspaces = new HashMap<>();
    private final AtomicReference<CompletableFuture<Void>> currentWatcher = new AtomicReference<>();
    private final ExecutorService executor;
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile boolean isShutdown;

    public ShardingService(Context context) {
        this.context = context;
        this.executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("kr.sharding-service-%d").build()
        );
        this.journal = new Journal(context);
        this.lastOffset = new AtomicLong(this.journal.getConsumer().getLatestIndex(JournalName.shardEvents(context.getMember())));
    }

    public void start() {
        executor.submit(new ShardEventsJournalWatcher());
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                LOGGER.error("Sharding service has failed to start background threads");
            }
        } catch (InterruptedException e) {
            throw new KronotopException(e);
        }
        LOGGER.info("Sharding service has been started");
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return context;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        currentWatcher.get().cancel(true);
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn(String.format("%s cannot be stopped gracefully", NAME));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadShardFromFDB(BaseTask task) {
        Shard shard = context.getLogicalDatabase().getShards().compute(task.getShardId(),
                (key, value) -> Objects.requireNonNullElseGet(
                        value, () -> new OnHeapShardImpl(task.getShardId()))
        );
        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ShardLoader shardLoader = new ShardLoader(context, shard);
                for (DataStructure dataStructure : DataStructure.values()) {
                    shardLoader.load(tr, dataStructure);
                }
                break;
            } catch (CompletionException e) {
                if (e.getCause() instanceof FDBException) {
                    String message = RESPError.decapitalize(e.getCause().getMessage());
                    if (message.equalsIgnoreCase(RESPError.TRANSACTION_TOO_OLD_MESSAGE)) {
                        continue;
                    }
                }
                throw e;
            }
        }
        LOGGER.info("ShardId: {} has been loaded from FoundationDB", task.getShardId());
    }

    private String getTaskId(String journal, long offset) {
        return String.format("%s:%d", journal, offset);
    }

    private synchronized void processShardingTask() {
        context.getFoundationDB().run(tr -> {
            String journalName = JournalName.shardEvents(context.getMember());
            while (true) {
                // Try to consume the latest event.
                Event event = journal.getConsumer().consumeEvent(tr, journalName, lastOffset.get() + 1);
                if (event == null)
                    // There is nothing to process
                    return null;

                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                try {
                    BaseTask baseTask = objectMapper.readValue(event.getValue(), BaseTask.class);
                    LOGGER.info("New sharding task has been received. ShardId: {}, TaskType: {}", baseTask.getShardId(), baseTask.getType());
                    if (baseTask.getType() == TaskType.ASSIGN_SHARD) {
                        AssignShardTask task = objectMapper.readValue(event.getValue(), AssignShardTask.class);
                        loadShardFromFDB(task);

                        // Complete the task.
                        String taskId = getTaskId(journalName, event.getOffset());
                        journal.getPublisher().publish(JournalName.coordinatorEvents(), new TaskCompletedEvent(baseTask.getShardId(), taskId));
                    } else if (baseTask.getType() == TaskType.REASSIGN_SHARD) {
                        ReassignShardTask task = objectMapper.readValue(event.getValue(), ReassignShardTask.class);
                    } else {
                        LOGGER.error("Unknown task. ShardId: {}, TaskType: {}", baseTask.getShardId(), baseTask.getType());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // Processed the event successfully. Forward the offset.
                lastOffset.set(event.getOffset());
            }
        });
    }

    private class ShardEventsJournalWatcher implements Runnable {
        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                JournalMetadata journalMetadata = journal.getConsumer().getJournalMetadata(JournalName.shardEvents(context.getMember()));
                CompletableFuture<Void> watcher = tr.watch(journalMetadata.getJournalKey());
                tr.commit().join();
                currentWatcher.set(watcher);
                latch.countDown();
                try {
                    processShardingTask();
                    watcher.join();
                } catch (CancellationException e) {
                    LOGGER.debug("{} watcher has been cancelled", JournalName.shardEvents(context.getMember()));
                    return;
                }

                processShardingTask();
            } catch (Exception e) {
                LOGGER.error("Error while listening journal: {}", JournalName.shardEvents(context.getMember()), e);
            } finally {
                if (!isShutdown) {
                    executor.execute(this);
                }
            }
        }
    }
}
