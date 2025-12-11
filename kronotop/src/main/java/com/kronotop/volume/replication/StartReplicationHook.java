/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.replication;

import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.sharding.ShardKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartReplicationHook implements RoutingEventHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartReplicationHook.class);
    private final ReplicationService service;

    public StartReplicationHook(ReplicationService service) {
        this.service = service;
    }

    @Override
    public void run(ShardKind shardKind, int shardId) {
        try {
            service.startReplication(shardKind, shardId, false);
            LOGGER.info(
                    "Initializing volume replication: ShardKind={}, ShardId={}",
                    shardKind, shardId
            );
        } catch (ReplicationAlreadyExistsException | NoRouteFoundException exp) {
            LOGGER.debug("Skipping replication initialization for {}-{}: {}",
                    shardKind, shardId, exp.getMessage());
        }
    }
}
