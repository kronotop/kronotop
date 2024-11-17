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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.VersionstampUtils;
import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.volume.VolumeConfigGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static com.kronotop.volume.Subspaces.MEMBER_REPLICATION_SLOT_SUBSPACE;

public class CreateReplicationSlotHook implements RoutingEventHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateReplicationSlotHook.class);
    private final Context context;
    private final RoutingService routing;

    public CreateReplicationSlotHook(Context context) {
        this.context = context;
        this.routing = context.getService(RoutingService.NAME);
    }

    private void registerReplicationSlot(
            Transaction tr,
            DirectorySubspace volumeSubspace,
            ShardKind shardKind,
            int shardId
    ) {
        Tuple tuple = Tuple.from(
                MEMBER_REPLICATION_SLOT_SUBSPACE,
                shardKind.name(),
                shardId,
                context.getMember().getId()
        );
        tr.mutate(
                MutationType.SET_VERSIONSTAMPED_VALUE,
                volumeSubspace.pack(tuple),
                Tuple.from(Versionstamp.incomplete()).packWithVersionstamp()
        );
    }

    @Override
    public void run(ShardKind shardKind, int shardId) {
        DirectorySubspace volumeSubspace = new VolumeConfigGenerator(context, shardKind, shardId).createOrOpenVolumeSubspace();
        ReplicationConfigNG replicationConfig = new ReplicationConfigNG(
                volumeSubspace,
                shardKind,
                shardId,
                false);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlotNG.newSlot(tr, replicationConfig);
            registerReplicationSlot(tr, volumeSubspace, shardKind, shardId);

            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.commit().join();

            byte[] trVersion = future.join();
            Versionstamp slotId = Versionstamp.complete(trVersion);
            LOGGER.info("Created replication slot with SlotID: {} for ShardKind: {}, ShardId: {}",
                    VersionstampUtils.base64Encode(slotId),
                    shardKind,
                    shardId
            );
        }
    }
}
