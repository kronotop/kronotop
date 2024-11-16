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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.redis.storage.RedisShardVolumeConfig;
import com.kronotop.volume.VolumeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateReplicationSlotHook implements RoutingEventHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateReplicationSlotHook.class);
    private final Context context;
    private final RoutingService routing;

    public CreateReplicationSlotHook(Context context) {
        this.context = context;
        this.routing = context.getService(RoutingService.NAME);
    }

    @Override
    public void run(ShardKind shardKind, int shardId) {
        VolumeConfig volumeConfig = RedisShardVolumeConfig.newVolumeConfig(context, shardId);
        ReplicationConfigNG config = new ReplicationConfigNG(volumeConfig, shardKind, shardId, context.getMember().getId());
        ReplicationSlotNG.newSlot(context.getFoundationDB(), config);
        LOGGER.info("Created replication slot for ShardKind: {} ShardId: {}",
                shardKind,
                shardId
        );
    }
}
