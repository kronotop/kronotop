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

package com.kronotop.volume.replication;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.VersionstampUtils;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeConfigGenerator;
import com.kronotop.volume.VolumeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class CreateReplicationSlotHook implements RoutingEventHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateReplicationSlotHook.class);
    private final Context context;

    public CreateReplicationSlotHook(Context context) {
        this.context = context;
    }

    @Override
    public void run(ShardKind shardKind, int shardId) {
        VolumeConfig volumeConfig = new VolumeConfigGenerator(context, shardKind, shardId).volumeConfig();
        ReplicationConfig config = new ReplicationConfig(
                volumeConfig,
                shardKind,
                shardId,
                ReplicationStage.SNAPSHOT);

        // Create or open the volume, it's required for sync replication.
        VolumeService volumeService = context.getService(VolumeService.NAME);
        try {
            // newVolume opens or creates the volume for replication.
            volumeService.newVolume(volumeConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (ReplicationMetadata.findSlotId(context, config) != null) {
            // Nothing to do, we already have a replication slot for this config.
            return;
        }

        Versionstamp slotId = ReplicationMetadata.newReplication(context, config);
        LOGGER.info("Created replication slot with SlotID: {} for ShardKind: {}, ShardId: {}",
                VersionstampUtils.base32HexEncode(slotId),
                shardKind,
                shardId
        );

        ReplicationService replicationService = context.getService(ReplicationService.NAME);
        Replication replication = replicationService.newReplication(slotId, config);
        try {
            replication.start();
            LOGGER.info("Replication started with SlotID: {} for ShardKind: {}, ShardId: {}",
                    VersionstampUtils.base32HexEncode(slotId),
                    shardKind,
                    shardId
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
