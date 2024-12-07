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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.DirectorySubspaceCache;
import com.kronotop.JSONUtils;
import com.kronotop.cluster.sharding.ShardStatus;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MembershipUtils {

    /**
     * Determines if the cluster is initialized by checking a specific key in the database.
     *
     * @param tr The transaction used to perform the database operation.
     * @param clusterMetadataSubspace The directory subspace where cluster metadata is stored.
     * @return true if the cluster is initialized, false otherwise.
     */
    public static boolean isClusterInitialized(Transaction tr, DirectorySubspace clusterMetadataSubspace) {
        byte[] key = clusterMetadataSubspace.pack(Tuple.from(MembershipConstants.CLUSTER_INITIALIZED));
        return MembershipUtils.isTrue(tr.get(key).join());
    }

    /**
     * Loads the shard status from the specified subspace within a transaction.
     *
     * @param tr            The transaction used to read from the database.
     * @param shardSubspace The specific directory subspace containing the shard status.
     * @return The shard status as a ShardStatus enum value. If the status is not found, returns ShardStatus.INOPERABLE.
     */
    public static ShardStatus loadShardStatus(Transaction tr, DirectorySubspace shardSubspace) {
        byte[] statusKey = shardSubspace.pack(Tuple.from(MembershipConstants.SHARD_STATUS_KEY));
        byte[] statusValue = tr.get(statusKey).join();
        if (statusValue == null) {
            return ShardStatus.INOPERABLE;
        }
        return ShardStatus.valueOf(new String(statusValue).toUpperCase());
    }

    /**
     * Loads the primary member ID for a shard from the specified subspace within a transaction.
     *
     * @param tr            The transaction used to read from the database.
     * @param shardSubspace The specific directory subspace containing the primary member information.
     * @return The primary member ID as a string, or null if no primary member information is found.
     */
    public static String loadPrimaryMemberId(Transaction tr, DirectorySubspace shardSubspace) {
        byte[] key = shardSubspace.pack(Tuple.from(MembershipConstants.ROUTE_PRIMARY_MEMBER_KEY));
        return tr.get(key).thenApply((value) -> {
            if (value == null) {
                return null;
            }
            return new String(value);
        }).join();
    }

    /**
     * Loads the IDs of the standby members from the specified subspace within a transaction.
     *
     * @param tr            The transaction used to read from the database.
     * @param shardSubspace The specific directory subspace containing the standby member information.
     * @return A set of strings representing the standby member IDs, or an empty set if no standby member information is found.
     */
    public static Set<String> loadStandbyMemberIds(Transaction tr, DirectorySubspace shardSubspace) {
        byte[] key = shardSubspace.pack(Tuple.from(MembershipConstants.ROUTE_STANDBY_MEMBER_KEY));
        return tr.get(key).thenApply((value) -> {
            if (value == null) {
                return new HashSet<String>();
            }
            List<String> items = Arrays.asList(JSONUtils.readValue(value, String[].class));
            return new HashSet<>(items);
        }).join();
    }

    /**
     * Checks if the given byte array represents the constant TRUE value defined in MembershipConstants.
     *
     * @param data The byte array to be checked.
     * @return true if the byte array matches MembershipConstants.TRUE; false otherwise.
     */
    public static boolean isTrue(byte[] data) {
        return Arrays.equals(data, MembershipConstants.TRUE);
    }

    public static Set<String> loadSyncStandbyMemberIds(Transaction tr, DirectorySubspace shardSubspace) {
        byte[] key = shardSubspace.pack(Tuple.from(MembershipConstants.ROUTE_SYNC_STANDBY_MEMBERS));
        return tr.get(key).thenApply((value) -> {
            if (value == null) {
                return new HashSet<String>();
            }
            List<String> items = Arrays.asList(JSONUtils.readValue(value, String[].class));
            return new HashSet<>(items);
        }).join();
    }
}
