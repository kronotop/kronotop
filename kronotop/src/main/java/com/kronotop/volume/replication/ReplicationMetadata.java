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
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.VersionstampUtils;

import java.util.concurrent.CompletableFuture;

import static com.kronotop.volume.Subspaces.MEMBER_REPLICATION_SLOT_SUBSPACE;

public class ReplicationMetadata {

    private static void registerReplicationSlot(
            Context context,
            Transaction tr,
            ReplicationConfigNG config
    ) {
        Tuple tuple = Tuple.from(
                MEMBER_REPLICATION_SLOT_SUBSPACE,
                context.getMember().getId(),
                config.shardKind().name(),
                config.shardId()
        );
        tr.mutate(
                MutationType.SET_VERSIONSTAMPED_VALUE,
                config.volumeSubspace().pack(tuple),
                Tuple.from(Versionstamp.incomplete()).packWithVersionstamp()
        );
    }

    public static Versionstamp newReplication(Context context, ReplicationConfigNG config) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot.newSlot(tr, config);
            registerReplicationSlot(context, tr, config);

            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.commit().join();

            byte[] trVersion = future.join();
            return Versionstamp.complete(trVersion);
        }
    }

    public static Versionstamp findSlotId(Context context, ReplicationConfigNG config) {
        Tuple tuple = Tuple.from(
                MEMBER_REPLICATION_SLOT_SUBSPACE,
                context.getMember().getId(),
                config.shardKind(),
                config.shardId()
        );
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] versionBytes = tr.get(config.volumeSubspace().pack(tuple)).join();
            return Versionstamp.fromBytes(versionBytes);
        }
    }

    public static String stringifySlotId(Versionstamp slotId) {
        return VersionstampUtils.base64Encode(slotId);
    }
}
