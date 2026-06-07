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
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.DataStructureKind;
import com.kronotop.KronotopException;
import com.kronotop.bucket.vector.VectorGraphIndexGroup;
import com.kronotop.bucket.vector.VectorGraphIndexRegistry;
import com.kronotop.cluster.BaseBroadcastEvent;
import com.kronotop.cluster.BroadcastEventKind;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.DirectoryUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.KeyWatcher;
import com.kronotop.journal.Consumer;
import com.kronotop.journal.ConsumerConfig;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalName;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import com.kronotop.namespace.NoSuchNamespaceException;
import com.kronotop.worker.Worker;
import com.kronotop.worker.WorkerTag;
import com.kronotop.worker.WorkerUtil;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Watches bucket metadata change events and maintains version tracking for shards.
 *
 * <p>This watcher subscribes to the BUCKET_METADATA_EVENTS journal and processes metadata
 * change events as they occur. For each event, it updates the last seen version for all
 * shards where this member is the primary owner. This version tracking serves as a witness
 * mechanism to coordinate metadata synchronization across the cluster.
 *
 * <h2>How It Works</h2>
 *
 * <p><b>Event Consumption:</b>
 * The watcher uses a journal Consumer to read BucketMetadataEvent entries from FoundationDB.
 * It maintains a per-member consumer offset to ensure events are processed exactly once,
 * even across restarts (RESUME offset mode).
 *
 * <p><b>Watch Mechanism:</b>
 * Uses FoundationDB's watch feature on the journal trigger key to efficiently wait for new
 * events. When the trigger fires, the watcher processes all available events in a batch
 * before resuming the watch. This reduces polling overhead while maintaining low latency.
 *
 * <p><b>Version Tracking:</b>
 * For each metadata change event, the watcher:
 * <ol>
 *   <li>Loads the current BucketMetadata to get the latest version number
 *   <li>Iterates through all bucket shards
 *   <li>For shards where this member is the primary, updates lastSeenVersions/[metadataId]
 *   <li>Stores the version as an 8-byte little-endian long value
 * </ol>
 *
 * <p>This creates a distributed record of which versions each shard has witnessed, enabling
 * coordination protocols like {@link BucketMetadataVersionBarrier} to ensure all relevant
 * shards have seen a particular metadata version before proceeding with operations.
 *
 * <p><b>Lifecycle:</b>
 * The watcher runs in a background thread. It maintains
 * a shutdown latch to allow graceful termination with a 5-second timeout. On shutdown, it
 * unwatches all keys and stops the consumer, ensuring clean resource cleanup.
 *
 * <p><b>Error Handling:</b>
 * Individual event processing errors break the inner consumption loop, leaving the failed
 * event unconsumed so it will be retried on the next watcher trigger. The outer watcher loop
 * applies a 1-second backoff on errors to prevent tight spinning. Transaction operations use
 * automatic retries via Resilience4j. If a bucket no longer exists, NoSuchBucketException is
 * silently ignored since the metadata change may have been a deletion.
 */
public class BucketEventsWatcher implements Runnable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(BucketEventsWatcher.class);
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
     * Creates a new watcher for the current cluster member.
     *
     * @param context the system context providing services and configuration
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

    /**
     * Opens the lastSeenVersions subspace for a shard, caching the result.
     *
     * @param tr      the FoundationDB transaction
     * @param shardId the shard identifier
     * @return the directory subspace for storing version witness records
     */
    private DirectorySubspace openLastSeenVersionsSubspace(Transaction tr, int shardId) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                shards().
                bucket().
                shard(shardId).
                lastSeenVersions();
        return subspaces.computeIfAbsent(shardId,
                (ignored) -> context.getDirectoryLayer().open(tr, directory.toList()).join()
        );
    }

    /**
     * Updates the last seen version for shards owned by this member.
     *
     * @param tr         the FoundationDB transaction
     * @param metadataId the bucket metadata identifier
     * @param value      the version number encoded as 8-byte little-endian
     */
    private void updateLastSeenVersion(Transaction tr, UUID metadataId, byte[] value) {
        for (int shardId : context.getShardRegistry().getShardIds(ShardKind.BUCKET)) {
            Route route = routing.findRoute(ShardKind.BUCKET, shardId);
            if (route == null) {
                LOGGER.error("Bucket shard '{}' could not be found", shardId);
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

    /**
     * Processes a bucket metadata change event and records the version.
     *
     * @param tr    the FoundationDB transaction
     * @param event the journal event containing metadata change details
     */
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
                Path bucketVectorDir = VectorGraphIndexGroup.resolveVectorDir(bucketService.getBucketDataDir())
                        .resolve(metadata.uuid().toString());
                DirectoryUtil.deleteRecursively(bucketVectorDir);

                String tag = WorkerTag.generate(DataStructureKind.BUCKET, metadata.name());
                List<Worker> workers = context.getWorkerRegistry().get(metadata.namespace(), tag);
                planCache.invalidateBucket(evt.namespace(), evt.id());

                // Try to reduce entropy
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

                Path indexDir = VectorGraphIndexGroup.resolveVectorDir(bucketService.getBucketDataDir())
                        .resolve(evt.bucketId().toString())
                        .resolve(Long.toString(evt.indexId()));
                DirectoryUtil.deleteRecursively(indexDir);

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
        } else {
            throw new KronotopException(String.format("Unknown %s kind: %s", JournalName.BUCKET_EVENTS, base.kind()));
        }
    }

    /**
     * Fetches and processes all available metadata events from the journal.
     */
    private void fetchBucketEvents() {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                while (!shutdown) {
                    // Try to consume the latest event.
                    Event event = consumer.consume(tr);
                    if (event == null) {
                        break;
                    }

                    try {
                        processBucketEvent(tr, event);
                        consumer.markConsumed(tr, event);
                    } catch (Exception e) {
                        LOGGER.error("Bucket event processing failed – will retry on next trigger", e);
                        break;
                    }
                }
                tr.commit().join();
            }
        });
    }

    /**
     * Runs the watcher loop, processing events until shutdown is requested.
     */
    @Override
    public void run() {
        try {
            consumer.start();

            while (!shutdown) {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    CompletableFuture<Void> watcher = keyWatcher.watch(tr, context.getJournal().getJournalMetadata(journalName).trigger());
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
                        // unprocessed, leaving the dropped index in the registry —
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
     * Initiates shutdown and waits for graceful termination.
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
