/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.namespace;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.cluster.BaseBroadcastEvent;
import com.kronotop.cluster.BroadcastEventKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.KeyWatcher;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.journal.*;
import com.kronotop.namespace.handlers.NamespaceHandler;
import com.kronotop.namespace.handlers.NamespaceRemovedEvent;
import com.kronotop.server.ServerKind;
import com.kronotop.worker.Worker;
import com.kronotop.worker.WorkerUtil;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

public class NamespaceService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Namespace";
    private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceService.class);
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final Consumer namespaceEventsConsumer;
    private final ExecutorService executor;
    private DirectorySubspace memberSubspace;
    private volatile boolean shutdown;

    public NamespaceService(Context context) {
        super(context, NAME);

        String consumerId = String.format("%s-member:%s",
                JournalName.NAMESPACE_EVENTS.getValue(),
                context.getMember().getId()
        );
        ConsumerConfig config = new ConsumerConfig(consumerId, JournalName.NAMESPACE_EVENTS.getValue(), ConsumerConfig.Offset.LATEST);
        this.namespaceEventsConsumer = new Consumer(context, config);

        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("kr.namespace-%d").build();
        this.executor = Executors.newSingleThreadExecutor(factory);

        // Register handlers here
        handlerMethod(ServerKind.EXTERNAL, new NamespaceHandler(this));
    }

    public void start() {
        namespaceEventsConsumer.start();

        NamespaceEventsJournalWatcher namespaceEventsJournalWatcher = new NamespaceEventsJournalWatcher();
        executor.submit(namespaceEventsJournalWatcher);
        namespaceEventsJournalWatcher.waitUntilStarted();
    }

    private void openMemberSubspace() {
        if (memberSubspace != null) return;

        // cache member subspace, no need to be concurrent. Simple, procedural flow.
        List<String> subpath = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                members().
                member(context.getMember().getId()).toList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            memberSubspace = DirectoryLayer.getDefault().open(tr, subpath).join();
        }
    }

    /**
     * Records the last seen namespace version for this member if all workers have terminated.
     * Only writes the version when workers.isEmpty(), indicating all workers have gracefully
     * shut down and removed themselves from the registry via completion hooks.
     */
    private void setLastSeenNamespaceVersion(Transaction tr, NamespaceRemovedEvent event) {
        NavigableMap<String, Map<String, List<Worker>>> workers =
                context.getWorkerRegistry().get(event.namespace());
        if (workers.isEmpty()) {
            openMemberSubspace();
            NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, context.getClusterName(), event);
        }
    }

    /**
     * Processes a namespace removal event by invalidating caches, shutting down associated workers,
     * and recording the namespace version. Workers remove themselves from the registry upon completion.
     * This operation is idempotent and designed to be invoked multiple times across distributed nodes.
     */
    private void processNamespaceRemovedEvent(Transaction tr, byte[] data) {
        NamespaceRemovedEvent event = JSONUtil.readValue(data, NamespaceRemovedEvent.class);
        context.getBucketMetadataCache().invalidate(event.namespace());
        context.getSessionStore().invalidateOpenNamespaces(event.namespace());

        NavigableMap<String, Map<String, List<Worker>>> workers =
                context.getWorkerRegistry().get(event.namespace());

        List<Worker> allWorkers = workers.values().stream()
                .flatMap(m -> m.values().stream())
                .flatMap(List::stream)
                .toList();

        // This may fail, no worries. The operator should call namespace purge <namespace> command again.
        WorkerUtil.shutdownThenAwait(event.namespace(), allWorkers);

        setLastSeenNamespaceVersion(tr, event);
    }

    private void processNamespaceEvent(Transaction tr, Event event) {
        BaseBroadcastEvent baseBroadcastEvent = JSONUtil.readValue(event.value(), BaseBroadcastEvent.class);
        if (Objects.requireNonNull(baseBroadcastEvent.kind()) == BroadcastEventKind.NAMESPACE_REMOVED_EVENT) {
            processNamespaceRemovedEvent(tr, event.value());
        } else {
            throw new KronotopException("Unknown broadcast event: " + baseBroadcastEvent.kind());
        }
    }

    private synchronized void fetchNamespaceEvents() {
        Retry retry = TransactionUtils.retry(10, Duration.ofMillis(100));
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                while (true) {
                    // Try to consume the latest event.
                    Event event;
                    try {
                        event = namespaceEventsConsumer.consume(tr);
                        if (event == null) {
                            LOGGER.trace("No more Namespace events to consume");
                            break;
                        }
                    } catch (IllegalConsumerStateException exp) {
                        LOGGER.warn("Namespace event consumer is in an illegal state while consuming: {}", exp.getMessage());
                        break;
                    }

                    try {
                        processNamespaceEvent(tr, event);
                        namespaceEventsConsumer.markConsumed(tr, event);
                    } catch (IllegalConsumerStateException exp) {
                        LOGGER.warn("Namespace event consumer is in an illegal state while marking consumed: {}", exp.getMessage());
                        break;
                    } catch (Exception e) {
                        // TODO: endless loop
                        LOGGER.error("Namespace event processing failed for event={} – skipping", event, e);
                    }
                }
                tr.commit().join();
                LOGGER.debug("Namespace event batch committed");
            }
        });
    }

    @Override
    public void shutdown() {
        shutdown = true;
        keyWatcher.unwatchAll();
        namespaceEventsConsumer.stop();

        // executor
        ExecutorServiceUtil.shutdownNowThenAwaitTermination(executor);
    }

    private class NamespaceEventsJournalWatcher implements Runnable {
        /**
         * Latch to signal when the watcher has started and is ready.
         */
        private final CountDownLatch latch = new CountDownLatch(1);

        /**
         * Blocks until the watcher has started and performed its initial event fetch.
         *
         * @throws RuntimeException if the wait is interrupted
         */
        public void waitUntilStarted() {
            try {
                latch.await(); // Wait until the watcher is started
            } catch (InterruptedException exp) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Wait interrupted", exp);
            }
        }

        @Override
        public void run() {
            if (shutdown) {
                return;
            }

            while (!shutdown) {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    CompletableFuture<Void> watcher = keyWatcher.watch(tr, context.getJournal().getJournalMetadata(JournalName.NAMESPACE_EVENTS.getValue()).trigger());
                    tr.commit().join();
                    try {
                        // Try to fetch the latest events before start waiting
                        fetchNamespaceEvents();
                        latch.countDown();
                        watcher.join();
                    } catch (CancellationException e) {
                        LOGGER.debug("{} watcher has been cancelled", JournalName.NAMESPACE_EVENTS);
                        return;
                    }
                    // A new event is ready to read
                    fetchNamespaceEvents();
                } catch (Exception e) {
                    LOGGER.error("Error while watching journal: {}", JournalName.NAMESPACE_EVENTS, e);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                }
            }
        }
    }
}
