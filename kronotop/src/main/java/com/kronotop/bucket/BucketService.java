/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.ShardOwnerService;
import com.kronotop.bucket.handlers.*;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.bucket.index.VectorIndexDefinition;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceRoutineMetrics;
import com.kronotop.bucket.vector.*;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.RoutingEventKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.core.InfoCollector;
import com.kronotop.internal.DirectoryUtil;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.metrics.InfoReport;
import com.kronotop.server.ServerKind;
import com.kronotop.server.SessionAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Stream;

/*
We have been given the scientific knowledge, the technical ability and the materials to pursue the exploration of the universe.
To ignore these great resources would be a corruption of a God-given ability.
-- Wernher von Braun
 */

public class BucketService extends ShardOwnerService<BucketShard> implements KronotopService {
    public static final String NAME = "Bucket";
    private static final Logger LOGGER = LoggerFactory.getLogger(BucketService.class);
    private final RoutingService routing;
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder().setNameFormat("kr.bucket-service-%d").build()
    );
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final BucketEventsWatcher bucketEventsWatcher;
    // The default ShardSelector is RoundRobinShardSelector.
    private final ShardSelector shardSelector = new RoundRobinShardSelector();
    private final VectorGraphIndexRegistry vectorGraphRegistry = new VectorGraphIndexRegistry();
    private final ExecutorService vectorGraphExecutor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setNameFormat("kr.vector-%d").build()
    );
    private final CollatorCache collatorCache = new CollatorCache();
    private final QueryExecutor queryExecutor;
    private final PlanCache planCache = new PlanCache();
    private final BucketMetrics metrics = new BucketMetrics();
    private final Planner planner;
    private final DelayQueue<VectorFlushTask> vectorFlushQueue = new DelayQueue<>();
    private final VectorFlushMaintainer vectorFlushMaintainer;
    private final long vectorFlushThresholdBytes;
    private final int pqTrainingThreshold;
    private final int pqSubspaceDivisor;
    private final int maxScanCandidates;
    private final float defaultOverquery;
    private final Path bucketDataDir;

    public BucketService(Context context) {
        super(context, NAME);
        this.routing = context.getService(RoutingService.NAME);
        this.vectorFlushThresholdBytes = context.getConfig().getLong("bucket.vector.flush_threshold_bytes");
        this.pqTrainingThreshold = context.getConfig().getInt("bucket.vector.pq_training_threshold");
        this.pqSubspaceDivisor = context.getConfig().getInt("bucket.vector.pq_subspace_divisor");
        this.maxScanCandidates = context.getConfig().getInt("bucket.vector.max_scan_candidates");
        this.defaultOverquery = (float) context.getConfig().getDouble("bucket.vector.default_overquery");
        this.bucketDataDir = context.getDataDir().resolve("bucket");

        context.setBucketMetadataCache(new BucketMetadataCache(context));

        handlerMethod(ServerKind.EXTERNAL, new BucketCreateHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketInsertHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketQueryHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketExplainHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketAdvanceHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketDeleteHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketUpdateHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketCloseHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketCursorsHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketIndexHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketRemoveHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketPurgeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketLocateHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketListHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketVectorHandler(this));

        routing.registerHook(RoutingEventKind.HAND_OVER_SHARD_OWNERSHIP, new HandOverShardOwnershipHook());
        routing.registerHook(RoutingEventKind.INITIALIZE_BUCKET_SHARD, new InitializeBucketShardHook());

        this.bucketEventsWatcher = new BucketEventsWatcher(context, planCache);
        this.planner = new Planner(planCache, metrics);
        this.queryExecutor = new QueryExecutor(this);
        this.vectorFlushMaintainer = new VectorFlushMaintainer(context);
    }

    public PlanCache getPlanCache() {
        return planCache;
    }

    public BucketMetrics getMetrics() {
        return metrics;
    }

    public DelayQueue<VectorFlushTask> getVectorFlushQueue() {
        return vectorFlushQueue;
    }

    @Override
    public void collectInfo(InfoCollector collector) {
        int workers = 0;
        long lastRun = 0;
        for (BucketShard shard : getServiceContext().shards().values()) {
            if (shard.isClosed()) {
                continue;
            }
            for (IndexMaintenanceRoutineMetrics metrics : shard.indexMaintenanceMetrics()) {
                workers++;
                lastRun = Math.max(lastRun, metrics.getLatestExecution());
            }
        }
        collector.put(InfoReport.BUCKET_SECTION, "plan_cache_size", planCache.size());
        collector.put(InfoReport.BUCKET_SECTION, "plan_cache_hits", metrics.getPlanCacheHits());
        collector.put(InfoReport.BUCKET_SECTION, "plan_cache_misses", metrics.getPlanCacheMisses());
        int[] openCursors = new int[1];
        context.getSessionStore().forEach(session -> {
            openCursors[0] += session.attr(SessionAttributes.BUCKET_READ_QUERY_CONTEXTS).get().size();
            openCursors[0] += session.attr(SessionAttributes.BUCKET_DELETE_QUERY_CONTEXTS).get().size();
            openCursors[0] += session.attr(SessionAttributes.BUCKET_UPDATE_QUERY_CONTEXTS).get().size();
        });
        collector.put(InfoReport.BUCKET_SECTION, "open_cursors", openCursors[0]);
        collector.put(InfoReport.BUCKET_SECTION, "bucket_inserts", metrics.getInserts());
        collector.put(InfoReport.BUCKET_SECTION, "bucket_queries", metrics.getQueries());
        collector.put(InfoReport.BUCKET_SECTION, "bucket_updates", metrics.getUpdates());
        collector.put(InfoReport.BUCKET_SECTION, "bucket_deletes", metrics.getDeletes());
        collector.put(InfoReport.BUCKET_SECTION, "bucket_advances", metrics.getAdvances());
        collector.put(InfoReport.BUCKET_SECTION, "bucket_vector_searches", metrics.getVectorSearches());
        collector.put(InfoReport.BUCKET_SECTION, "bucket_purges", metrics.getPurges());
        collector.put(InfoReport.BUCKET_SECTION, "bucket_removes", metrics.getRemoves());
        collector.put(InfoReport.BUCKET_SECTION, "index_maintenance_workers", workers);
        collector.put(InfoReport.BUCKET_SECTION, "index_maintenance_processed_entries",
                metrics.getIndexMaintenanceProcessedEntries());
        collector.put(InfoReport.BUCKET_SECTION, "index_maintenance_retried_conflicts",
                metrics.getIndexMaintenanceRetriedConflicts());
        collector.put(InfoReport.BUCKET_SECTION, "index_maintenance_last_run", lastRun);

        int[] indexes = new int[1];
        long[] bytes = new long[1];
        int[] onDisk = new int[1];
        vectorGraphRegistry.forEachGroup(group -> {
            group.forEachOnHeap(index -> {
                indexes[0]++;
                bytes[0] += index.ramBytesUsed();
            });
            onDisk[0] += group.getOnDiskIndexes().size();
        });
        collector.put(InfoReport.VECTOR_SECTION, "vector_indexes", indexes[0]);
        collector.put(InfoReport.VECTOR_SECTION, "vector_bytes_used", bytes[0]);
        collector.put(InfoReport.VECTOR_SECTION, "vector_on_disk_indexes", onDisk[0]);
        collector.put(InfoReport.VECTOR_SECTION, "vector_flush_queue_size", vectorFlushQueue.size());
        collector.put(InfoReport.VECTOR_SECTION, "vector_flush_failures", metrics.getVectorFlushFailures());
    }

    public Route findRoute(int shardId) {
        return routing.findRoute(ShardKind.BUCKET, shardId);
    }

    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }

    public CollatorCache getCollatorCache() {
        return collatorCache;
    }

    public Planner getPlanner() {
        return planner;
    }

    public ShardSelector getShardSelector() {
        return shardSelector;
    }

    public VectorGraphIndexRegistry getVectorGraphRegistry() {
        return vectorGraphRegistry;
    }

    /**
     * Returns the current generation id of the vector index group for the given index.
     * The id identifies the on-heap graph generation that will receive this write.
     *
     * @throws VectorIndexNotReadyException if the group is not registered yet
     */
    public byte[] takeVectorIndexGenerationId(BucketMetadata metadata, long indexId) {
        VectorGraphIndexGroup group = vectorGraphRegistry.get(metadata.namespace(), metadata.name(), indexId);
        if (group == null) {
            throw new VectorIndexNotReadyException(metadata.namespace(), metadata.name(), indexId);
        }
        return group.takeGenerationId();
    }

    public ExecutorService getVectorGraphExecutor() {
        return vectorGraphExecutor;
    }

    public long getVectorFlushThresholdBytes() {
        return vectorFlushThresholdBytes;
    }

    public int getPqTrainingThreshold() {
        return pqTrainingThreshold;
    }

    public int getPqSubspaceDivisor() {
        return pqSubspaceDivisor;
    }

    public int getMaxScanCandidates() {
        return maxScanCandidates;
    }

    public float getDefaultOverquery() {
        return defaultOverquery;
    }

    public Path getBucketDataDir() {
        return bucketDataDir;
    }

    /**
     * Retrieves the BucketShard instance associated with the specified shard ID.
     *
     * @param shardId the unique identifier of the shard to retrieve
     * @return the BucketShard associated with the provided shard ID,
     * or null if no shard is found for the given ID
     */
    public BucketShard getShard(int shardId) {
        return getServiceContext().shards().get(shardId);
    }

    /**
     * Initializes the bucket shard for the specified shard ID if the current member is
     * either the primary or one of the standby members for the shard. The initialized
     * shard is added to the service context's shard map.
     *
     * @param shardId the unique identifier of the shard to be initialized
     */
    private void initializeBucketShardsIfOwned(int shardId) {
        Route route = routing.findRoute(ShardKind.BUCKET, shardId);
        if (route == null) {
            return;
        }
        if (route.primary().equals(context.getMember()) || route.standbys().contains(context.getMember())) {
            BucketShard shard = new BucketShardImpl(context, shardId);
            getServiceContext().shards().put(shardId, shard);
            shard.setOperable(true);
            shardSelector.add(shard);
        }
    }

    /**
     * Creates the graph index group of a vector index. If the index has no on-disk files and no
     * mutation log entries, the group is ready at once. Otherwise, the on-disk indexes are opened and
     * the mutation log is replayed in the background; the group becomes ready when that work ends.
     */
    public VectorGraphIndexGroup bootstrapVectorGroup(BucketMetadata metadata, VectorIndex vectorIndex) {
        if (!hasOnDiskIndexes(metadata, vectorIndex) && !hasMutationLogEntries(vectorIndex)) {
            return new VectorGraphIndexGroup(context, metadata, vectorIndex);
        }

        CompletableFuture<Void> bootstrapFuture = new CompletableFuture<>();
        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex, bootstrapFuture);

        vectorGraphExecutor.submit(() -> {
            try {
                VectorIndexDefinition definition = vectorIndex.definition();
                List<OnDiskVectorGraphIndex> onDiskIndexes = VectorGraphIndexGroup.openOnDiskIndexes(
                        context,
                        vectorIndex.subspace(),
                        bucketDataDir,
                        VectorGraphIndexGroup.ownerShardId(metadata),
                        metadata.uuid(),
                        definition.id(),
                        OnHeapVectorGraphIndex.toSimilarityFunction(definition.distance())
                );
                for (OnDiskVectorGraphIndex onDisk : onDiskIndexes) {
                    group.addOnDisk(onDisk);
                }

                VectorIndexCrashRecovery.recover(
                        context,
                        vectorIndex.subspace(),
                        group,
                        definition.dimensions(),
                        OnHeapVectorGraphIndex.toSimilarityFunction(definition.distance()),
                        vectorGraphExecutor,
                        pqTrainingThreshold,
                        pqSubspaceDivisor
                );
                group.markReady();
            } catch (Exception e) {
                LOGGER.error("Vector index bootstrap failed for bucket={}, indexId={}",
                        metadata.name(), vectorIndex.definition().id(), e);
                group.markFailed(e);
            }
        });

        return group;
    }

    private boolean hasOnDiskIndexes(BucketMetadata metadata, VectorIndex vectorIndex) {
        Path indexDir = VectorGraphIndexGroup
                .resolveVectorDir(bucketDataDir, VectorGraphIndexGroup.ownerShardId(metadata))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        if (!Files.isDirectory(indexDir)) {
            return false;
        }
        try (Stream<Path> stream = Files.list(indexDir)) {
            return stream.anyMatch(p -> p.toString().endsWith(".complete"));
        } catch (IOException e) {
            // Assume data exists so bootstrapVectorGroup takes the slow-path rather than skipping recovery.
            return true;
        }
    }

    /**
     * Deletes vector index files of the bucket from every owned shard. A null indexId deletes all indexes of the bucket.
     */
    public void deleteVectorIndexFiles(UUID bucketId, Long indexId) {
        for (int shardId : getServiceContext().shards().keySet()) {
            Path dir = VectorGraphIndexGroup.resolveVectorDir(bucketDataDir, shardId).resolve(bucketId.toString());
            if (indexId != null) {
                dir = dir.resolve(Long.toString(indexId));
            }
            DirectoryUtil.deleteRecursively(dir);
        }
    }

    private boolean hasMutationLogEntries(VectorIndex vectorIndex) {
        DirectorySubspace indexSubspace = vectorIndex.subspace();
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue()));
        try (Transaction tr = context.getTransactionManager().create()) {
            List<KeyValue> result = tr.getRange(
                    KeySelector.firstGreaterOrEqual(prefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix)),
                    1
            ).asList().join();
            return !result.isEmpty();
        }
    }

    @Override
    public void start() {
        for (int shardId : context.getShardRegistry().getShardIds(ShardKind.BUCKET)) {
            initializeBucketShardsIfOwned(shardId);
        }

        executor.submit(bucketEventsWatcher);
        Runnable evictionWorker = context.getBucketMetadataCache().createEvictionWorker(context::now, 1000 * 5 * 60);
        scheduler.scheduleAtFixedRate(evictionWorker, 1, 1, TimeUnit.MINUTES);
        scheduler.scheduleWithFixedDelay(
                () -> vectorGraphRegistry.forEachGroup(VectorGraphIndexGroup::retryPendingOnDiskDeletes),
                5, 5, TimeUnit.SECONDS
        );
        vectorFlushMaintainer.start();
    }

    @Override
    public void shutdown() {
        // The shutdown order matters. Every executor that can touch the vector graph indexes
        // (scheduler with the pending on-disk delete retry, vectorGraphExecutor) must stop before
        // the flush maintainer enters drain mode. The flush maintainer must be the last one to stop,
        // so the final flush sees no concurrent mutations.
        try {
            bucketEventsWatcher.shutdown();

            for (BucketShard shard : getServiceContext().shards().values()) {
                shard.close();
            }

            executor.shutdownNow();
            if (!executor.awaitTermination(
                    ExecutorServiceUtil.DEFAULT_TIMEOUT,
                    ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
            )) {
                LOGGER.warn("{} service cannot be stopped gracefully (executor)", NAME);
            }

            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(
                    ExecutorServiceUtil.DEFAULT_TIMEOUT,
                    ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
            )) {
                LOGGER.warn("{} service cannot be stopped gracefully (scheduler)", NAME);
            }

            vectorGraphExecutor.shutdown();
            if (!vectorGraphExecutor.awaitTermination(
                    ExecutorServiceUtil.DEFAULT_TIMEOUT,
                    ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
            )) {
                LOGGER.warn("{} service cannot be stopped gracefully (vectorGraphPool)", NAME);
            }

            // Try to flush all on-heap vector indexes. shutdown method will block until all indexes are flushed to
            // the disk. See bucket.vector.flush_drain_timeout_seconds
            vectorFlushMaintainer.setDrainMode();
            vectorGraphRegistry.enqueueAllForFlush();
            vectorFlushMaintainer.shutdown();

            vectorGraphRegistry.closeAll();
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        }
    }

    /**
     * Routing hook that initializes a bucket shard when this member becomes its primary or standby.
     * Ignores events for other shard kinds.
     */
    private class InitializeBucketShardHook implements RoutingEventHook {
        @Override
        public void run(ShardKind shardKind, int shardId) {
            if (shardKind != ShardKind.BUCKET) {
                return;
            }
            initializeBucketShardsIfOwned(shardId);
        }
    }

    /**
     * Routing hook that relinquishes a bucket shard when ownership moves away from this member. It
     * marks the shard inoperable, closes it, and drops it from the shard selector. Ignores events for
     * other shard kinds.
     */
    private class HandOverShardOwnershipHook implements RoutingEventHook {
        @Override
        public void run(ShardKind shardKind, int shardId) {
            if (shardKind != ShardKind.BUCKET) {
                return;
            }
            BucketShard shard = getShard(shardId);
            shard.setOperable(false);
            shard.close();
            shardSelector.remove(shard);
        }
    }
}
