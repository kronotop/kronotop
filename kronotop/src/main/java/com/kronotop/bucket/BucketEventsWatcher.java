/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.DataModelKind;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.bucket.vector.VectorGraphIndexRegistry;
import com.kronotop.cluster.BaseBroadcastEvent;
import com.kronotop.cluster.BroadcastEventKind;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.KeyWatcher;
import com.kronotop.journal.*;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import com.kronotop.namespace.NoSuchNamespaceException;
import com.kronotop.volume.Prefix;
import com.kronotop.worker.Worker;
import com.kronotop.worker.WorkerTag;
import com.kronotop.worker.WorkerUtil;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Applies bucket events from the BUCKET_EVENTS journal on this member.
 */
public class BucketEventsWatcher implements Runnable {
    static final int BUCKET_EVENTS_BATCH_SIZE = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(BucketEventsWatcher.class);
    private final String journalName = JournalName.BUCKET_EVENTS.getValue();
    private final Context context;
    private final Consumer consumer;
    private final Map<Integer, DirectorySubspace> subspaces = new HashMap<>();
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final RoutingService routing;
    private final PlanCache planCache;
    private volatile boolean shutdown;

    /**
     * Creates a watcher for this cluster member.
     *
     * @param context   the service context
     * @param planCache the plan cache to invalidate when a bucket changes
     */
    public BucketEventsWatcher(Context context, PlanCache planCache) {
        this.context = context;
        this.planCache = planCache;
        this.routing = context.getService(RoutingService.NAME);

        String consumerId = String.format("%s-member:%s",
                journalName,
                context.getMember().getId()
        );
        ConsumerConfig config = new ConsumerConfig(consumerId,
                journalName,
                ConsumerConfig.Offset.RESUME
        );
        this.consumer = new Consumer(context, config);
    }

    private DirectorySubspace openLastSeenVersionsSubspace(Transaction tr, int shardId) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                shards().
                shard(shardId).
                lastSeenVersions();
        return subspaces.computeIfAbsent(shardId,
                (ignored) -> context.getDirectoryLayer().open(tr, directory.toList()).join()
        );
    }

    private void updateLastSeenVersion(Transaction tr, UUID metadataId, byte[] value) {
        for (int shardId : context.getShardRegistry().getShardIds()) {
            Route route = routing.findRoute(shardId);
            if (route == null) {
                LOGGER.error("Shard '{}' could not be found", shardId);
                continue;
            }
            if (route.primary().equals(context.getMember())) {
                DirectorySubspace subspace = openLastSeenVersionsSubspace(tr, shardId);
                byte[] key = subspace.pack(Tuple.from(metadataId));
                tr.set(key, value);
            }
        }
    }

    private void updateLastSeenVersionIfNoWorkers(Transaction tr, BucketMetadata metadata, String tag) {
        List<Worker> workers = context.getWorkerRegistry().get(metadata.namespace(), tag);
        if (workers.isEmpty()) {
            byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(metadata.version()).array();
            updateLastSeenVersion(tr, metadata.uuid(), value);
        }
    }

    private void processStaleBucketEvent(Transaction tr, StaleBucketEvent evt) {
        UUID uuid = UUID.fromString(evt.pointer());
        BucketService bucketService = context.getService(BucketService.NAME);

        // Local leftovers. Both calls are no-ops when there is nothing to remove.
        context.getBucketMetadataCache().invalidateBinding(Prefix.fromLong(evt.prefix()));
        bucketService.deleteVectorIndexFiles(uuid, null);

        List<String> registryPath = BucketMetadataUtil.registryPath(context.getClusterName(), uuid);
        DirectorySubspace registry;
        try {
            registry = context.getDirectoryLayer().open(tr, registryPath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                // Another member removed the registry already.
                return;
            }
            throw e;
        }

        List<Integer> shards = BucketMetadataHeader.read(tr, registry).shards();
        boolean hosted = shards.stream().anyMatch(shardId -> bucketService.getShard(shardId) != null);
        if (!hosted) {
            return;
        }

        // Task back-pointers live under the registry, so clear the tasks before the directory goes.
        IndexTaskUtil.clearBucketTasks(new TransactionalContext(context, tr), registry);
        for (int shardId : shards) {
            DirectorySubspace subspace = openLastSeenVersionsSubspace(tr, shardId);
            tr.clear(subspace.pack(Tuple.from(uuid)));
        }
        context.getDirectoryLayer().removeIfExists(tr, registryPath).join();
    }

    private void processBucketEvent(Transaction tr, Event event) {
        BaseBroadcastEvent base = JSONUtil.readValue(event.value(), BaseBroadcastEvent.class);
        if (Objects.requireNonNull(base.kind()) == BroadcastEventKind.BUCKET_METADATA_UPDATED_EVENT) {
            BucketMetadataUpdatedEvent evt = JSONUtil.readValue(event.value(), BucketMetadataUpdatedEvent.class);
            try {
                // forceOpen will cache the BucketMetadata again if it's not removed.
                BucketMetadata metadata = BucketMetadataUtil.forceOpen(context, tr.snapshot(), evt.namespace(), evt.bucket());
                byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(metadata.version()).array();
                updateLastSeenVersion(tr, metadata.uuid(), value);
                planCache.invalidateBucket(evt.namespace(), evt.id());
            } catch (NoSuchBucketException | NoSuchNamespaceException | NamespaceBeingRemovedException ignored) {
                // BucketBeingRemovedException cannot occur here: forceOpen passes force=true,
                // which bypasses the removed-bucket check in doOpen.
            }
        } else if (Objects.requireNonNull(base.kind()) == BroadcastEventKind.BUCKET_REMOVED_EVENT) {
            BucketRemovedEvent evt = JSONUtil.readValue(event.value(), BucketRemovedEvent.class);
            try {
                BucketMetadata metadata = BucketMetadataUtil.forceOpen(context, tr.snapshot(), evt.namespace(), evt.bucket());

                // Clean up all in-memory vector indexes for this bucket
                BucketService bucketService = context.getService(BucketService.NAME);
                VectorGraphIndexRegistry registry = bucketService.getVectorGraphRegistry();
                registry.remove(evt.namespace(), evt.bucket());

                // Delete vector index disk files for the entire bucket
                bucketService.deleteVectorIndexFiles(metadata.uuid(), null);

                String tag = WorkerTag.generate(DataModelKind.BUCKET, metadata.name());
                List<Worker> workers = context.getWorkerRegistry().get(metadata.namespace(), tag);
                planCache.invalidateBucket(evt.namespace(), evt.id());

                // Invalidate bucket metadata cache to prevent stale prefix on recreation
                context.getBucketMetadataCache().invalidate(evt.namespace(), evt.bucket());

                // This may fail, no worries. The operator should call bucket.purge <bucket-name> command again.
                WorkerUtil.shutdownThenAwait(context.getWorkerRegistry(), metadata.namespace(), workers);

                updateLastSeenVersionIfNoWorkers(tr, metadata, tag);
            } catch (NoSuchBucketException | NoSuchNamespaceException | NamespaceBeingRemovedException ignored) {
                // BucketBeingRemovedException cannot occur here: forceOpen passes force=true,
                // which bypasses the removed-bucket check in doOpen.
            }
        } else if (Objects.requireNonNull(base.kind()) == BroadcastEventKind.VECTOR_INDEX_DROPPED_EVENT) {
            VectorIndexDroppedEvent evt = JSONUtil.readValue(event.value(), VectorIndexDroppedEvent.class);
            try {
                BucketService bucketService = context.getService(BucketService.NAME);
                VectorGraphIndexRegistry registry = bucketService.getVectorGraphRegistry();
                registry.remove(evt.namespace(), evt.bucket(), evt.indexId());

                bucketService.deleteVectorIndexFiles(evt.bucketId(), evt.indexId());

                BucketMetadata metadata = BucketMetadataUtil.forceOpen(context, tr.snapshot(), evt.namespace(), evt.bucket());
                byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(metadata.version()).array();
                updateLastSeenVersion(tr, metadata.uuid(), value);
                planCache.invalidateBucket(evt.namespace(), evt.id());
            } catch (NoSuchBucketException | NoSuchNamespaceException | NamespaceBeingRemovedException ignored) {
            }
        } else if (Objects.requireNonNull(base.kind()) == BroadcastEventKind.INDEX_STATISTICS_UPDATED_EVENT) {
            IndexStatisticsUpdatedEvent evt = JSONUtil.readValue(event.value(), IndexStatisticsUpdatedEvent.class);
            planCache.invalidateBucket(evt.namespace(), evt.id());
            context.getBucketMetadataCache().invalidate(evt.namespace(), evt.bucket());
        } else if (Objects.requireNonNull(base.kind()) == BroadcastEventKind.STALE_BUCKET_EVENT) {
            StaleBucketEvent evt = JSONUtil.readValue(event.value(), StaleBucketEvent.class);
            processStaleBucketEvent(tr, evt);
        } else {
            throw new KronotopException(String.format("Unknown %s kind: %s", JournalName.BUCKET_EVENTS, base.kind()));
        }
    }

    private boolean fetchBucketEventsBatch() {
        try (Transaction tr = context.getTransactionManager().create()) {
            boolean more = false;
            for (int i = 0; i < BUCKET_EVENTS_BATCH_SIZE && !shutdown; i++) {
                // Try to consume the latest event.
                Event event = consumer.consume(tr);
                if (event == null) {
                    break;
                }

                try {
                    processBucketEvent(tr, event);
                    consumer.markConsumed(tr, event);
                } catch (Exception e) {
                    LOGGER.error("Bucket event processing failed, will retry on next trigger", e);
                    break;
                }
                more = i == BUCKET_EVENTS_BATCH_SIZE - 1;
            }
            tr.commit().join();
            return more;
        }
    }

    private void fetchBucketEvents() {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        // Each batch commits on its own, so a long journal never exceeds the transaction limits.
        while (!shutdown) {
            boolean more = retry.executeSupplier(this::fetchBucketEventsBatch);
            if (!more) {
                break;
            }
        }
    }

    @Override
    public void run() {
        try {
            consumer.start();

            while (!shutdown) {
                try (Transaction tr = context.getTransactionManager().create()) {
                    JournalService journal = context.getService(JournalService.NAME);
                    CompletableFuture<Void> watcher = keyWatcher.watch(tr, journal.getJournalMetadata(journalName).trigger());
                    // TODO: retry if this fails
                    tr.commit().join();
                    try {
                        // Try to fetch the latest events before start waiting
                        fetchBucketEvents();
                        watcher.join();
                    } catch (CancellationException e) {
                        LOGGER.debug("{} watcher has been cancelled", JournalName.BUCKET_EVENTS);
                        // Drain any pending events before exiting. During shutdown,
                        // BucketService calls flushAll() after this watcher stops.
                        // Without this drain, a VectorIndexDroppedEvent could remain
                        // unprocessed, leaving the dropped index in the registry, so
                        // flushAll() would then re-persist its on-heap data to disk
                        // as stale files.
                        LOGGER.debug("Draining remaining {} events before shutdown", JournalName.BUCKET_EVENTS);
                        fetchBucketEvents();
                        return;
                    }
                    // A new event is ready to read
                    fetchBucketEvents();
                } catch (Exception e) {
                    LOGGER.error("Error while watching journal: {}", JournalName.BUCKET_EVENTS, e);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                }
            }
        } finally {
            shutdownLatch.countDown();
        }
    }

    /**
     * Stops the watcher loop and waits until it exits.
     *
     * @throws KronotopException if the calling thread is interrupted while waiting
     */
    public void shutdown() {
        shutdown = true;
        keyWatcher.unwatchAll();
        try {
            if (!shutdownLatch.await(10, TimeUnit.SECONDS)) {
                LOGGER.warn("{} watcher cannot be stopped gracefully", JournalName.BUCKET_EVENTS);
            }
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        }
        consumer.stop();
    }
}
