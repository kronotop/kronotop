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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static com.kronotop.volume.Subspaces.MEMBER_REPLICATION_SLOT_SUBSPACE;

/**
 * The ReplicationMetadata class provides methods for managing replication slots within
 * the transactional context of a Kronotop instance.
 * <p>
 * This includes creating new replication slots, finding existing slots,
 * and converting replication metadata to string representations.
 */
public class ReplicationMetadata {

    /**
     * Registers a replication slot in a transactional context with the specified replication configuration.
     *
     * @param context the context of the Kronotop instance.
     * @param tr the transaction in which the replication slot registration occurs.
     * @param config the replication configuration, including shard kind, shard ID, and volume configuration.
     */
    private static void registerReplicationSlot(
            Context context,
            Transaction tr,
            ReplicationConfig config
    ) {
        Tuple tuple = Tuple.from(
                MEMBER_REPLICATION_SLOT_SUBSPACE,
                context.getMember().getId(),
                config.shardKind().name(),
                config.shardId()
        );
        tr.mutate(
                MutationType.SET_VERSIONSTAMPED_VALUE,
                config.volumeConfig().subspace().pack(tuple),
                Tuple.from(Versionstamp.incomplete()).packWithVersionstamp()
        );
    }

    /**
     * Creates a new replication slot with the given configuration. If a replication slot
     * with the same configuration already exists, an {@link IllegalArgumentException}
     * is thrown.
     *
     * @param context the context of the Kronotop instance.
     * @param config the replication configuration.
     * @return the versionstamp of the newly created replication slot.
     * @throws IllegalArgumentException if a replication slot with the same configuration already exists.
     */
    public static Versionstamp newReplication(Context context, ReplicationConfig config) {
        if (ReplicationMetadata.findSlotId(context, config) != null) {
            throw new IllegalArgumentException("Replication already exists");
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot.newSlot(tr, config);
            registerReplicationSlot(context, tr, config);

            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.commit().join();

            byte[] trVersion = future.join();
            return Versionstamp.complete(trVersion);
        }
    }

    /**
     * Finds the replication slot ID based on the provided context and replication configuration.
     * <p>
     * You should know that this method tries to find a replication slot registered for the current member.
     * <p>
     * @param context the context of the Kronotop instance.
     * @param config  the replication configuration. This includes details such as shard kind, shard ID, and volume configuration.
     * @return the versionstamp of the replication slot if found; otherwise, null.
     */
    public static Versionstamp findSlotId(Context context, ReplicationConfig config) {
        Tuple tuple = Tuple.from(
                MEMBER_REPLICATION_SLOT_SUBSPACE,
                context.getMember().getId(),
                config.shardKind().name(),
                config.shardId()
        );
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = tr.get(config.volumeConfig().subspace().pack(tuple)).join();
            if (value == null) {
                return null;
            }

            byte[] trVersion = Arrays.copyOfRange(value, 0, 10);
            return Versionstamp.complete(trVersion);
        }
    }

    /**
     * Converts a Versionstamp object into its Base64 string representation.
     *
     * @param slotId the Versionstamp object to encode.
     * @return the Base64 encoded string representation of the Versionstamp object.
     */
    public static String stringifySlotId(Versionstamp slotId) {
        return VersionstampUtils.base64Encode(slotId);
    }
}
