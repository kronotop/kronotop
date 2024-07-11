/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.cluster.sharding;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.Context;
import com.kronotop.KeyWatcher;
import com.kronotop.KronotopService;
import com.kronotop.cluster.coordinator.Route;
import com.kronotop.cluster.coordinator.RoutingTable;
import com.kronotop.cluster.coordinator.events.TaskCompletedEvent;
import com.kronotop.cluster.coordinator.tasks.AssignShardTask;
import com.kronotop.cluster.coordinator.tasks.BaseTask;
import com.kronotop.cluster.coordinator.tasks.ReassignShardTask;
import com.kronotop.cluster.coordinator.tasks.TaskType;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalMetadata;
import com.kronotop.journal.JournalName;
import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.storage.impl.OnHeapShardImpl;
import com.kronotop.redis.storage.persistence.DataStructure;
import com.kronotop.redis.storage.persistence.Persistence;
import com.kronotop.redis.storage.persistence.ShardLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ShardingService is a service for managing shards in a distributed database system.
 * <p>
 * This service is responsible for loading shards from FoundationDB,
 * dropping previously owned shards, and making shards operable based on routing tables.
 * <p>
 * The service listens for sharding tasks from a journal and handles them accordingly.
 * <p>
 * Shards can be loaded, dropped, and made operable by calling the respective methods.
 */
public class ShardingService implements KronotopService {
    public static final String NAME = "Sharding";
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardingService.class);
    private final Context context;
    private final AtomicReference<byte[]> latestShardEventsVersionstamp = new AtomicReference<>();
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final ExecutorService executor;
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile boolean isShutdown;

    public ShardingService(Context context) {
        this.context = context;
        this.executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("kr.sharding-%d").build()
        );

        context.getFoundationDB().run(tr -> {
            byte[] key = context.getJournal().getConsumer().getLatestEventKey(tr, JournalName.shardEvents(context.getMember()));
            this.latestShardEventsVersionstamp.set(key);
            return null;
        });
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
        keyWatcher.unwatch(context.getJournal().getJournalMetadata(JournalName.shardEvents(context.getMember())).getTrigger());
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn(String.format("%s cannot be stopped gracefully", NAME));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads a shard from FoundationDB.
     *
     * @param task The task containing the shard ID of the shard to be loaded.
     */
    private void loadShardFromFDB(BaseTask task) {
        if (context.getLogicalDatabase().getShards().containsKey(task.getShardId())) {
            LOGGER.warn("ShardId: {} could not be loaded from FoundationDB because it's already exists", task.getShardId());
            return;
        }

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
        shard.setReadOnly(true);
    }

    /**
     * Drops shards that were previously owned by the current member and have been assigned to a different owner in the new routing table.
     *
     * @param oldRoutingTable The old routing table.
     * @param newRoutingTable The new routing table.
     */
    public void dropPreviouslyOwnedShards(RoutingTable oldRoutingTable, RoutingTable newRoutingTable) {
        if (oldRoutingTable == null || newRoutingTable == null) {
            return;
        }
        int numberOfShards = context.getConfig().getInt("cluster.number_of_shards");
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            Route oldRoute = oldRoutingTable.getRoute(shardId);
            if (oldRoute == null) {
                continue;
            }
            if (oldRoute.getMember().equals(context.getMember())) {
                // It belonged to this member. Check the new owner.
                Route newRoute = newRoutingTable.getRoute(shardId);
                if (!newRoute.getMember().equals(context.getMember())) {
                    LOGGER.info("Dropping ShardId: {}", shardId);
                    context.getLogicalDatabase().getShards().remove(shardId);
                }
            }
        }
    }

    /**
     * Makes the shards operable by setting their read-only flag to false and operable flag to true,
     * based on the old and new routing tables.
     *
     * @param oldRoutingTable The old routing table.
     * @param newRoutingTable The new routing table.
     */
    public void makeShardsOperable(RoutingTable oldRoutingTable, RoutingTable newRoutingTable) {
        if (oldRoutingTable == null) {
            oldRoutingTable = new RoutingTable();
        }
        int numberOfShards = context.getConfig().getInt("cluster.number_of_shards");
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            Route oldRoute = oldRoutingTable.getRoute(shardId);
            if (oldRoute == null || !oldRoute.getMember().equals(context.getMember())) {
                Route newRoute = newRoutingTable.getRoute(shardId);
                if (newRoute != null && newRoute.getMember().equals(context.getMember())) {
                    context.getLogicalDatabase().getShards().computeIfPresent(shardId, (id, shard) -> {
                        shard.setReadOnly(false);
                        shard.setOperable(true);
                        return shard;
                    });
                }
            }
        }
    }

    /**
     * Processes sharding tasks by consuming and handling events from a journal.
     * This method is synchronized to ensure that only one thread can process tasks at a time.
     * <p>
     * The method retrieves the latest event from the shard events journal
     * and handles the event based on the type of the task.
     * <p>
     * If the task is an ASSIGN_SHARD task, the method loads the shard from FoundationDB
     * and completes the task by publishing a TaskCompletedEvent to the coordinator events journal.
     * <p>
     * If the task is a REASSIGN_SHARD task, the method sets the shard as read-only,
     * processes any pending persistence operations, and completes the task by publishing a TaskCompletedEvent
     * to the coordinator events journal.
     * <p>
     * If the task is of an unknown type, the method logs an error.
     * <p>
     * Finally, the method updates the latestShardEventsVersionstamp to the key of the processed event.
     */
    private synchronized void processShardingTask() {
        context.getFoundationDB().run(tr -> {
            String journalName = JournalName.shardEvents(context.getMember());
            while (true) {
                // Try to consume the latest event.
                Event event = context.getJournal().getConsumer().consumeNext(tr, journalName, latestShardEventsVersionstamp.get());
                if (event == null)
                    return null;

                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                try {
                    BaseTask baseTask = objectMapper.readValue(event.getValue(), BaseTask.class);
                    LOGGER.info("{} has been received", baseTask);
                    if (baseTask.getType() == TaskType.ASSIGN_SHARD) {
                        AssignShardTask task = objectMapper.readValue(event.getValue(), AssignShardTask.class);
                        loadShardFromFDB(task);

                        // Complete the task.
                        context.getJournal().getPublisher().publish(JournalName.coordinatorEvents(), new TaskCompletedEvent(baseTask));
                    } else if (baseTask.getType() == TaskType.REASSIGN_SHARD) {
                        ReassignShardTask task = objectMapper.readValue(event.getValue(), ReassignShardTask.class);
                        Shard shard = context.getLogicalDatabase().getShards().get(task.getShardId());
                        if (shard != null) {
                            shard.setReadOnly(true);
                            if (shard.getPersistenceQueue().size() > 0) {
                                Persistence persistence = new Persistence(context, shard);
                                while (!persistence.isQueueEmpty()) {
                                    persistence.run();
                                }
                            }
                        }
                        context.getJournal().getPublisher().publish(JournalName.coordinatorEvents(), new TaskCompletedEvent(baseTask));
                    } else {
                        LOGGER.error("{} unknown task ", baseTask);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // Processed the event successfully. Forward the position.
                latestShardEventsVersionstamp.set(event.getKey());
            }
        });
    }

    /**
     * The ShardEventsJournalWatcher class represents a watcher for the shard events journal.
     * It implements the Runnable interface for running the watcher in a separate thread.
     *
     * @see Runnable
     */
    private class ShardEventsJournalWatcher implements Runnable {
        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                JournalMetadata journalMetadata = context.getJournal().getJournalMetadata(JournalName.shardEvents(context.getMember()));
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, journalMetadata.getTrigger());
                tr.commit().join();
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
