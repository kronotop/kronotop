// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.ServiceContext;
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

public class BucketService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Bucket";
    protected static final Logger LOGGER = LoggerFactory.getLogger(BucketService.class);
    private final ServiceContext<BucketShard> serviceContext;
    private final RoutingService routing;
    private final Planner planner;

    public BucketService(Context context) {
        super(context, NAME);
        this.serviceContext = context.getServiceContext(NAME);
        this.routing = context.getService(RoutingService.NAME);
        this.planner = new Planner(context);

        handlerMethod(ServerKind.EXTERNAL, new BucketInsertHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketQueryHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new QueryHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new BucketAdvanceHandler(this));

        routing.registerHook(RoutingEventKind.INITIALIZE_BUCKET_SHARD, new InitializeBucketShardHook());
    }

    public Planner getPlanner() {
        return planner;
    }

    public BucketShard getShard(int shardId) {
        return serviceContext.shards().get(shardId);
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
            BucketShard shard = new AbstractBucketShard(context, shardId);
            serviceContext.shards().put(shardId, shard);
        }
    }

    public void start() {
        int numberOfShards = context.getConfig().getInt("bucket.shards");
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
