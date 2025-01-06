/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.cluster.sharding.impl;

import com.kronotop.Context;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.Shard;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.volume.VolumeService;

public abstract class ShardImpl implements Shard {
    protected final Context context;
    protected final int id;
    protected final ShardKind shardKind;
    protected final VolumeService volumeService;
    protected final RoutingService routingService;

    public ShardImpl(Context context, ShardKind shardKind, int id) {
        this.context = context;
        this.shardKind = shardKind;
        this.id = id;
        this.volumeService = context.getService(VolumeService.NAME);
        this.routingService = context.getService(RoutingService.NAME);
    }

    @Override
    public ShardStatus status() {
        Route route = routingService.findRoute(ShardKind.REDIS, id);
        if (route == null) {
            return ShardStatus.INOPERABLE;
        }
        return route.shardStatus();
    }

    @Override
    public Integer id() {
        return id;
    }
}
