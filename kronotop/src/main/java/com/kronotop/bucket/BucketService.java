// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.kronotop.*;
import com.kronotop.bucket.handlers.BucketAdvanceHandler;
import com.kronotop.bucket.handlers.BucketInsertHandler;
import com.kronotop.bucket.handlers.BucketQueryHandler;
import com.kronotop.bucket.handlers.QueryHandler;
import com.kronotop.bucket.planner.Planner;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.RoutingEventKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.server.ServerKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketService extends ShardOwnerService<BucketShard> implements KronotopService {
    public static final String NAME = "Bucket";
    protected static final Logger LOGGER = LoggerFactory.getLogger(BucketService.class);
    private final int numberOfShards;
    private final RoutingService routing;
    private final Planner planner;

    // The default ShardSelector is RoundRobinShardSelector.
    private final ShardSelector shardSelector = new RoundRobinShardSelector();

    public BucketService(Context context) {
        super(context, NAME);
        this.routing = context.getService(RoutingService.NAME);
        this.planner = new Planner(context);
        this.numberOfShards = context.getConfig().getInt("bucket.shards");

        handlerMethod(ServerKind.EXTERNAL, new BucketInsertHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketQueryHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new QueryHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketAdvanceHandler(this));

        routing.registerHook(RoutingEventKind.INITIALIZE_BUCKET_SHARD, new InitializeBucketShardHook());
    }

    /**
     * Retrieves the ShardSelector instance associated with the BucketService.
     *
     * @return the ShardSelector instance for managing shard selection logic
     */
    public ShardSelector getShardSelector() {
        return shardSelector;
    }

    public Planner getPlanner() {
        return planner;
    }

    /**
     * Retrieves the BucketShard instance associated with the specified shard ID.
     *
     * @param shardId the unique identifier of the shard to retrieve
     * @return the BucketShard associated with the provided shard ID,
     *         or null if no shard is found for the given ID
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
    }

    private class InitializeBucketShardHook implements RoutingEventHook {

        @Override
        public void run(ShardKind shardKind, int shardId) {
            initializeBucketShardsIfOwned(shardId);
        }
    }
}
