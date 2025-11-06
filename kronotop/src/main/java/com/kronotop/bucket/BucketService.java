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

package com.kronotop.bucket;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.CachedTimeService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.ShardOwnerService;
import com.kronotop.bucket.handlers.*;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.RoutingEventKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.server.ServerKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/*
We have been given the scientific knowledge, the technical ability and the materials to pursue the exploration of the universe.
To ignore these great resources would be a corruption of a God-given ability.
-- Wernher von Braun
 */

public class BucketService extends ShardOwnerService<BucketShard> implements KronotopService {
    public static final String NAME = "Bucket";
    protected static final Logger LOGGER = LoggerFactory.getLogger(BucketService.class);
    private final int numberOfShards;
    private final RoutingService routing;
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder().setNameFormat("kr.bucket-service-%d").build()
    );
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final BucketMetadataWatcher bucketMetadataWatcher;
    // The default ShardSelector is RoundRobinShardSelector.
    private final ShardSelector shardSelector = new RoundRobinShardSelector();
    private final QueryExecutor queryExecutor;
    private final Planner planner;
    private volatile boolean shutdown;

    public BucketService(Context context) {
        super(context, NAME);
        this.routing = context.getService(RoutingService.NAME);
        this.numberOfShards = context.getConfig().getInt("bucket.shards");

        context.setBucketMetadataCache(new BucketMetadataCache(context));

        handlerMethod(ServerKind.EXTERNAL, new BucketInsertHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketQueryHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new QueryHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketAdvanceHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketDeleteHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketUpdateHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketCloseHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketIndexHandler(this));

        routing.registerHook(RoutingEventKind.HAND_OVER_SHARD_OWNERSHIP, new HandOverShardOwnershipHook());
        routing.registerHook(RoutingEventKind.INITIALIZE_BUCKET_SHARD, new InitializeBucketShardHook());

        this.bucketMetadataWatcher = new BucketMetadataWatcher(context);
        this.planner = new Planner();
        this.queryExecutor = new QueryExecutor(this);
    }

    public Route findRoute(int shardId) {
        return routing.findRoute(ShardKind.BUCKET, shardId);
    }

    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
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
     * Returns the number of shards managed by the service.
     *
     * @return the total number of shards.
     */
    public int getNumberOfShards() {
        return numberOfShards;
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

    public void start() {
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            initializeBucketShardsIfOwned(shardId);
        }

        executor.submit(bucketMetadataWatcher);
        CachedTimeService cachedTimeService = context.getService(CachedTimeService.NAME);
        Runnable evictionWorker = context.getBucketMetadataCache().createEvictionWorker(cachedTimeService, 1000 * 5 * 60);
        scheduler.scheduleAtFixedRate(evictionWorker, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public void shutdown() {
        try {
            shutdown = true;
            bucketMetadataWatcher.shutdown();

            for (BucketShard shard : getServiceContext().shards().values()) {
                shard.close();
            }

            executor.shutdownNow();
            if (!executor.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn("{} service cannot be stopped gracefully", NAME);
            }

            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn("{} service cannot be stopped gracefully", NAME);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
