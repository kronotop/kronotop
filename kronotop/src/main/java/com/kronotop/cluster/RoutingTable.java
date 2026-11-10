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

package com.kronotop.cluster;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the route of every shard in the cluster, keyed by shard id.
 */
public class RoutingTable {
    private final ConcurrentHashMap<Integer, Route> routes = new ConcurrentHashMap<>();

    /**
     * Returns the route of the given shard.
     *
     * @param shardId the shard id
     * @return the route, or null if the shard has no route
     */
    public Route get(int shardId) {
        return routes.get(shardId);
    }

    void set(int shardId, Route route) {
        routes.put(shardId, route);
    }
}
