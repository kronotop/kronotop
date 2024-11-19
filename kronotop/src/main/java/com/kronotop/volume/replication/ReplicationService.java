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

package com.kronotop.volume.replication;

import com.kronotop.BaseKronotopService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingEventKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Replication";
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationService.class);

    private final RoutingService routing;

    public ReplicationService(Context context) {
        super(context, NAME);

        this.routing = context.getService(RoutingService.NAME);
        this.routing.registerHook(RoutingEventKind.CREATE_REPLICATION_SLOT, new CreateReplicationSlotHook(context));
    }

    public void start() {
        for (ShardKind shardKind : ShardKind.values()) {
            if (shardKind.equals(ShardKind.REDIS)) {
                int shards = context.getConfig().getInt("redis.shards");
                for (int shardId = 0; shardId < shards; shardId++) {
                    Route route = routing.findRoute(shardKind, shardId);
                    if (route == null) {
                        // Not assigned yet
                        continue;
                    }

                    if (route.standbys().contains(context.getMember())) {
                        LOGGER.info("Replication ShardKind: {} ShardId: {}", shardKind, shardId);
                        //ReplicationConfig replicationConfig = new ReplicationConfig();
                        //ReplicationMetadata.findSlotId(context, ReplicationConfig)
                    }
                }
            } else {
                throw new IllegalArgumentException("Unsupported shard kind: " + shardKind);
            }
        }
    }
}
