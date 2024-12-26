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
import com.kronotop.Context;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;

/**
 * Utility class for handling shard operations within the Kronotop system.
 */
public class ShardUtils {

    public static void setShardStatus(
            Transaction tr,
            ShardStatus shardStatus,
            DirectorySubspace shardSubspace
    ) {
        byte[] key = shardSubspace.pack(Tuple.from(MembershipConstants.SHARD_STATUS_KEY));
        tr.set(key, shardStatus.name().getBytes());
    }

    /**
     * Sets the status of a specific shard in the DirectorySubspace cache.
     *
     * @param context     the context of the Kronotop instance, containing necessary utilities and configurations
     * @param tr          the transaction within which the shard status is being set
     * @param shardKind   the kind of the shard being updated (e.g., REDIS)
     * @param shardStatus the new status to be set for the shard (e.g., READONLY, READWRITE, INOPERABLE)
     * @param shardId     the identifier for the specific shard whose status is being updated
     */
    public static void setShardStatus(
            Context context,
            Transaction tr,
            ShardKind shardKind,
            ShardStatus shardStatus,
            int shardId
    ) {
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardKind, shardId);
        setShardStatus(tr, shardStatus, shardSubspace);
    }

    /**
     * Retrieves the status of a specific shard from the given DirectorySubspace within a transaction.
     *
     * @param tr            the transaction within which the shard status is being retrieved.
     * @param shardSubspace the DirectorySubspace representing the shard.
     * @return the status of the specified shard as a {@link ShardStatus} enum value.
     * @throws IllegalStateException if the shard status has not been set for the given shard.
     */
    public static ShardStatus getShardStatus(Transaction tr, DirectorySubspace shardSubspace) {
        byte[] key = shardSubspace.pack(Tuple.from(MembershipConstants.SHARD_STATUS_KEY));
        byte[] value = tr.get(key).join();
        if (value == null) {
            throw new IllegalStateException("ShardStatus has not been set for the given shard");
        }
        return ShardStatus.valueOf(new String(value));
    }

    /**
     * Retrieves the status of a specific shard in the DirectorySubspace cache.
     *
     * @param context   the context of the Kronotop instance, containing the necessary utilities and configurations.
     * @param tr        the transaction within which the shard status is being retrieved.
     * @param shardKind the kind of shard to be retrieved (e.g., REDIS).
     * @param shardId   the identifier for the specific shard whose status is being retrieved.
     * @return the status of the specified shard as a {@link ShardStatus} enum value.
     * @throws IllegalStateException if the shard status has not been set for the given shard.
     */
    public static ShardStatus getShardStatus(
            Context context,
            Transaction tr,
            ShardKind shardKind,
            int shardId
    ) {
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardKind, shardId);
        return getShardStatus(tr, shardSubspace);
    }
}
