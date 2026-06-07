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
import com.kronotop.bucket.vector.*;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.RoutingEventKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.server.ServerKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;

/*
We have been given the scientific knowledge, the technical ability and the materials to pursue the exploration of the universe.
To ignore these great resources would be a corruption of a God-given ability.
-- Wernher von Braun
 */

public class BucketService extends ShardOwnerService<BucketShard> implements KronotopService {
    public static final String NAME = "Bucket";
    protected static final Logger LOGGER = LoggerFactory.getLogger(BucketService.class);
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
    private final Planner planner;
    private final long vectorFlushThresholdBytes;
    private final int pqTrainingThreshold;
    private final int pqSubspaceDivisor;
    private final int maxScanCandidates;
    private final float defaultOverquery;
    private final Path bucketDataDir;
    private volatile boolean shuttingDown = false;

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
        handlerMethod(ServerKind.EXTERNAL, new QueryHandler(this));
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
        this.planner = new Planner(planCache);
        this.queryExecutor = new QueryExecutor(this);
    }

    public PlanCache getPlanCache() {
        return planCache;
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

    /**
     * Retrieves the ShardSelector instance associated with the BucketService.
     *
     * @return the ShardSelector instance for managing shard selection logic
     */
    public ShardSelector getShardSelector() {
        return shardSelector;
    }

    public VectorGraphIndexRegistry getVectorGraphRegistry() {
        return vectorGraphRegistry;
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

    public boolean isShuttingDown() {
        return shuttingDown;
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
     * Loads a vector graph index group from the disk and replays mutation log entries for crash recovery.
     * If no disk data and no mutation log entries exist, returns an immediately ready group (fast-path).
     * Otherwise, returns a not-yet-ready group and runs bootstrap in the background (slow-path).
     * Called exactly once per index because ConcurrentHashMap.computeIfAbsent guarantees a single
     * supplier invocation for a given key.
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
                        bucketDataDir,
                        metadata.uuid(),
                        definition.id(),
                        OnHeapVectorGraphIndex.toSimilarityFunction(definition.distance())
                );
                for (OnDiskVectorGraphIndex onDisk : onDiskIndexes) {
                    group.addOnDisk(onDisk);
                }

                OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                        context.getFoundationDB(),
                        vectorIndex.subspace(),
                        group,
                        definition.dimensions(),
                        OnHeapVectorGraphIndex.toSimilarityFunction(definition.distance()),
                        vectorGraphExecutor,
                        pqTrainingThreshold,
                        pqSubspaceDivisor
                );
                if (recovered != null) {
                    group.addOnHeap(recovered);
                }

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
                .resolveVectorDir(bucketDataDir)
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

    private boolean hasMutationLogEntries(VectorIndex vectorIndex) {
        DirectorySubspace indexSubspace = vectorIndex.subspace();
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue()));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> result = tr.getRange(
                    KeySelector.firstGreaterOrEqual(prefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix)),
                    1
            ).asList().join();
            return !result.isEmpty();
        }
    }

    public void start() {
        for (int shardId : context.getShardRegistry().getShardIds(ShardKind.BUCKET)) {
            initializeBucketShardsIfOwned(shardId);
        }

        executor.submit(bucketEventsWatcher);
        Runnable evictionWorker = context.getBucketMetadataCache().createEvictionWorker(context::now, 1000 * 5 * 60);
        scheduler.scheduleAtFixedRate(evictionWorker, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public void shutdown() {
        shuttingDown = true;
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

            try {
                vectorGraphRegistry.flushAll(bucketDataDir);
            } catch (Exception e) {
                LOGGER.error("Failed to flush vector graph indexes on shutdown", e);
            }

            vectorGraphRegistry.closeAll();
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        }
    }

    /**
     * A private class that implements the {@link RoutingEventHook} interface to handle
     * shard initialization events for bucket shards.
     * <p>
     * This hook is designed to initialize a bucket shard for a specific shard ID when a routing
     * event is triggered, provided the current member is eligible (either as the primary or a
     * standby member) to own and manage the shard.
     * <p>
     * The {@code initializeBucketShardsIfOwned} method is invoked within the {@code run}
     * method of the hook, which ensures that the bucket shard is appropriately initialized
     * in the service context if ownership conditions are satisfied.
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
     * HandOverShardOwnershipHook is a private implementation of the {@code RoutingEventHook} interface
     * within the {@code BucketService} class. This hook is invoked to handle the process of transitioning
     * ownership of a shard, ensuring it is properly managed and removed from operation.
     *
     * <p>This implementation focuses specifically on shards of kind {@code BUCKET}. If the shard kind
     * does not match {@code BUCKET}, the process is halted, and no further operations are performed.
     *
     * <p>The {@code run} method is executed during routing events, where it performs the following steps:
     * 1. Validates that the shard kind matches {@code BUCKET}.
     * 2. Retrieves the {@code BucketShard} instance associated with the given shard ID using
     * {@code getShard(int shardId)}.
     * 3. Sets the shard's operable state to {@code false} using {@code shard.setOperable(boolean operable)}.
     * 4. Removes the shard from the shard selector using {@code shardSelector.remove(BucketShard shard)}.
     *
     * <p>This functionality ensures that shards of kind {@code BUCKET} are appropriately disabled
     * and excluded from the shard selector during ownership transitions.
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
