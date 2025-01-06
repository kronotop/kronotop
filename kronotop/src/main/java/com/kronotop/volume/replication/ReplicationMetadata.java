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

package com.kronotop.volume.replication;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.VersionstampUtils;
import com.kronotop.cluster.sharding.ShardKind;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.kronotop.volume.Subspaces.ENTRY_SUBSPACE;
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
     * @param tr      the transaction in which the replication slot registration occurs.
     * @param config  the replication configuration, including shard kind, shard ID, and volume configuration.
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
     * @param config  the replication configuration.
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
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                if (ex.getCode() == 1020) {
                    // Conflict, retry...
                    return newReplication(context, config);
                }
            }
            throw e;
        }
    }

    /**
     * Finds the replication slot ID for the specified parameters within the provided transactional context.
     * It constructs a key based on the member ID, shard kind, and shard ID and attempts to fetch
     * the corresponding value from the given volume subspace in the transaction. If no value is found,
     * the method returns null.
     *
     * @param tr             the transaction in which the lookup is performed.
     * @param volumeSubspace the directory subspace representing the volume in which the replication slot exists.
     * @param memberId       the unique identifier for the member for which the replication slot is being searched.
     * @param shardKind      the kind of shard (e.g., REDIS) for which the replication slot is associated.
     * @param shardId        the ID of the shard for which the replication slot is being queried.
     * @return the Versionstamp representing the replication slot ID if found, or null if no slot exists.
     */
    public static Versionstamp findSlotId(
            Transaction tr,
            DirectorySubspace volumeSubspace,
            String memberId,
            ShardKind shardKind,
            int shardId
    ) {
        Tuple tuple = Tuple.from(
                MEMBER_REPLICATION_SLOT_SUBSPACE,
                memberId,
                shardKind.name(),
                shardId
        );
        byte[] value = tr.get(volumeSubspace.pack(tuple)).join();
        if (value == null) {
            return null;
        }

        byte[] trVersion = Arrays.copyOfRange(value, 0, 10);
        return Versionstamp.complete(trVersion);
    }

    /**
     * Finds the replication slot ID for a specified replication configuration within the context
     * of the Kronotop instance. This method internally delegates to the overloaded version of
     * findSlotId, using parameters derived from the provided context and replication configuration.
     *
     * @param context the context of the Kronotop instance.
     * @param config  the replication configuration, including the volume subspace, member ID,
     *                shard kind, and shard ID.
     * @return the Versionstamp representing the replication slot if found, or null if no slot exists.
     */
    public static Versionstamp findSlotId(Context context, ReplicationConfig config) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return findSlotId(
                    tr,
                    config.volumeConfig().subspace(),
                    context.getMember().getId(),
                    config.shardKind(),
                    config.shardId()
            );
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

    /**
     * Finds the latest versionstamped key within a Volume.
     * It queries the database for the latest key in the given subspace and extracts the versionstamp
     * from the key's tuple structure. If no keys exist in the subspace, the method returns null.
     *
     * @param context        the context of the Kronotop instance, used to access the FoundationDB instance.
     * @param volumeSubspace the directory subspace within which to search for the latest versionstamped key.
     * @return the latest Versionstamp found in the specified subspace, or null if no keys are found.
     */
    public static Versionstamp findLatestVersionstampedKey(Context context, DirectorySubspace volumeSubspace) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple tuple = Tuple.from(ENTRY_SUBSPACE);
            byte[] prefix = volumeSubspace.pack(tuple);
            List<KeyValue> items = tr.getRange(Range.startsWith(prefix), 1, true).asList().join();
            if (items.isEmpty()) {
                return null;
            }
            KeyValue keyValue = items.getFirst();
            Tuple unpackedKey = volumeSubspace.unpack(keyValue.getKey());
            return (Versionstamp) unpackedKey.get(2);
        }
    }
}
