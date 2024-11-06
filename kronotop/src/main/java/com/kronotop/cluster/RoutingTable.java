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

package com.kronotop.cluster;

import com.kronotop.cluster.sharding.ShardKind;

import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The RoutingTable class is responsible for maintaining routing information
 * for different kinds of shards in a Kronotop cluster.
 * <p>
 * It uses an EnumMap to store routes based on shard kinds and shard IDs.
 */
public class RoutingTable {
    private final EnumMap<ShardKind, ConcurrentHashMap<Integer, Route>> routes = new EnumMap<>(ShardKind.class);

    public RoutingTable() {
        for (ShardKind shardKind : ShardKind.values()) {
            routes.put(shardKind, new ConcurrentHashMap<>());
        }
    }

    /**
     * Retrieves the routing information for a specific shard based on its kind and ID.
     *
     * @param shardKind the kind of the shard, represented by an instance of ShardKind
     * @param shardId   the ID of the shard for which to retrieve the route information
     * @return the Route object containing the primary and standby members, or null if no route information is found
     */
    public Route get(ShardKind shardKind, int shardId) {
        return routes.get(shardKind).get(shardId);
    }

    /**
     * Sets the routing information for a specific shard based on its kind and ID.
     *
     * @param shardKind the kind of the shard, represented by an instance of ShardKind
     * @param shardId   the ID of the shard for which to set the route information
     * @param route     the Route object containing the primary and standby members
     */
    void set(ShardKind shardKind, int shardId, Route route) {
        routes.get(shardKind).put(shardId, route);
    }
}
