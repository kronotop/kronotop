/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.*;

import java.nio.file.Path;
import java.util.List;

/**
 * Static helpers that create shards and read or write the shard status.
 */
public class ShardUtil {

    public static KronotopDirectoryNode getShardVolumeDirectory(Context context, int shardId) {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                volume(Integer.toString(shardId));
    }

    /**
     * Opens the directory subspace of the volume of a shard in a new transaction.
     *
     * @param context the Kronotop context
     * @param shardId the shard ID
     * @return the directory subspace of the shard volume
     * @throws NoSuchVolumeException if the volume does not exist
     */
    public static DirectorySubspace openShardVolumeSubspace(Context context, int shardId) {
        return TransactionUtil.execute(context, tr -> VolumeMetadataUtil.openVolumeSubspace(
                context,
                tr,
                VolumeNames.format(shardId),
                getShardVolumeDirectory(context, shardId)
        ));
    }

    /**
     * Returns the local directory that holds the files of a shard.
     *
     * @param dataDir the data directory of the member
     * @param shardId the shard ID
     * @return the shard directory under the data directory
     */
    public static Path shardDataDir(Path dataDir, int shardId) {
        return dataDir.resolve("shards").resolve(Integer.toString(shardId));
    }

    /**
     * Builds the volume configuration of a shard with its default local directory.
     *
     * @param context the Kronotop context
     * @param shardId the shard ID
     * @return the configuration of the shard volume
     * @throws NoSuchVolumeException if the volume does not exist
     */
    public static VolumeConfig newShardVolumeConfig(Context context, int shardId) {
        DirectorySubspace subspace = openShardVolumeSubspace(context, shardId);
        String dataDir = shardDataDir(context.getDataDir(), shardId).toString();
        long segmentSize = context.getConfig().getLong("volume.segment_size");
        return new VolumeConfig(subspace, VolumeNames.format(shardId), dataDir, segmentSize);
    }

    public static void setShardStatus(
            Transaction tr,
            ShardStatus shardStatus,
            DirectorySubspace shardSubspace
    ) {
        byte[] key = shardSubspace.pack(Tuple.from(ShardConstants.SHARD_STATUS_KEY));
        tr.set(key, shardStatus.name().getBytes());
    }

    /**
     * Sets the status of the shard with the given ID.
     *
     * @param context     the Kronotop context
     * @param tr          the transaction to write with
     * @param shardStatus the new status
     * @param shardId     the shard ID
     * @throws com.kronotop.NoSuchDirectorySubspaceException if the shard has no directory
     */
    public static void setShardStatus(
            Context context,
            Transaction tr,
            ShardStatus shardStatus,
            int shardId
    ) {
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardId);
        setShardStatus(tr, shardStatus, shardSubspace);
    }

    /**
     * Returns the status stored in the given shard directory.
     *
     * @param tr            the transaction to read with
     * @param shardSubspace the directory of the shard
     * @return the shard status
     * @throws IllegalStateException if the shard has no status
     */
    public static ShardStatus getShardStatus(Transaction tr, DirectorySubspace shardSubspace) {
        byte[] key = shardSubspace.pack(Tuple.from(ShardConstants.SHARD_STATUS_KEY));
        byte[] value = tr.get(key).join();
        if (value == null) {
            throw new IllegalStateException("ShardStatus has not been set for the given shard");
        }
        return ShardStatus.valueOf(new String(value));
    }

    /**
     * Returns the status of the shard with the given ID.
     *
     * @param context the Kronotop context
     * @param tr      the transaction to read with
     * @param shardId the shard ID
     * @return the shard status
     * @throws IllegalStateException                         if the shard has no status
     * @throws com.kronotop.NoSuchDirectorySubspaceException if the shard has no directory
     */
    public static ShardStatus getShardStatus(
            Context context,
            Transaction tr,
            int shardId
    ) {
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardId);
        return getShardStatus(tr, shardSubspace);
    }

    private static void createIndexTasksDirectory(Context context, Transaction tr, int shardId) {
        List<String> layout = KronotopDirectory.
                kronotop().cluster(context.getClusterName()).metadata().
                shards().shard(shardId).buckets().maintenance().
                index().tasks().toList();
        context.getDirectoryLayer().create(tr, layout).join();
    }

    private static void createLastSeenVersionsDirectory(Context context, Transaction tr, int shardId) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                shards().
                shard(shardId).
                buckets().
                lastSeenVersions();
        context.getDirectoryLayer().create(tr, directory.toList()).join();
    }

    private static void createVolumeMetadata(Context context, Transaction tr, int shardId) {
        DirectorySubspace volumeSubspace = VolumeMetadataUtil.createVolumeSubspace(
                context,
                tr,
                VolumeNames.format(shardId),
                getShardVolumeDirectory(context, shardId)
        );
        VolumeSubspace vs = new VolumeSubspace(volumeSubspace);
        try {
            VolumeMetadataUtil.create(tr, vs);
        } catch (VolumeAlreadyExistsException exp) {
            throw new VolumeAlreadyExistsException(VolumeNames.format(shardId));
        }
    }

    /**
     * Creates a shard that starts as {@link ShardStatus#INOPERABLE}.
     *
     * @param context the Kronotop context
     * @param tr      the transaction to write with. The caller commits it.
     * @param shardId the ID of the new shard
     * @return the directory of the new shard
     * @throws java.util.concurrent.CompletionException if a shard with the given ID already exists
     */
    public static DirectorySubspace createShard(Context context, Transaction tr, int shardId) {
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                shards().
                shard(shardId);
        DirectorySubspace shardSubspace = subspace.create(tr, directory.excludeSubspace(subspace)).join();
        createIndexTasksDirectory(context, tr, shardId);
        createLastSeenVersionsDirectory(context, tr, shardId);
        createVolumeMetadata(context, tr, shardId);
        setShardStatus(tr, ShardStatus.INOPERABLE, shardSubspace);
        return shardSubspace;
    }
}
