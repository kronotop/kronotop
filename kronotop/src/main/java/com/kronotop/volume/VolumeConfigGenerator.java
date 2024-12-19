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

package com.kronotop.volume;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.typesafe.config.Config;

import java.nio.file.Path;
import java.util.concurrent.CompletionException;

/**
 * VolumeConfigGenerator is responsible for generating configuration settings for Volumes
 * within a Kronotop cluster. It interacts with the Kronotop and FoundationDB directory structure to
 * create or open directory subspaces for volume management.
 */
public class VolumeConfigGenerator {
    private final Context context;
    private final ShardKind shardKind;
    private final int shardId;

    public VolumeConfigGenerator(Context context, ShardKind shardKind, int shardId) {
        this.context = context;
        this.shardKind = shardKind;
        this.shardId = shardId;
    }

    /**
     * Retrieves the directory node corresponding to a Redis shard volume within the Kronotop directory structure.
     * <p>
     * This method constructs the path to the Redis shard volume based on the current cluster name and the shard ID.
     *
     * @return a KronotopDirectoryNode representing the path to the Redis shard volume directory.
     */
    private KronotopDirectoryNode getRedisShardVolumeDirectory() {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                redis().
                volume(Integer.toString(shardId));
    }

    /**
     * Creates or opens a volume subspace within the FoundationDB directory for a given Kronotop directory node.
     * This method ensures that a directory subspace is available for storing data associated with the specified
     * directory node. If a transaction error specific to conflict resolution occurs, the method will retry recursively.
     *
     * @param directory the KronotopDirectoryNode that represents the directory layout for which the subspace
     *                  is to be created or opened.
     * @return the DirectorySubspace associated with the provided KronotopDirectoryNode, representing the path
     * in the FoundationDB directory layer where volume data will be stored.
     * @throws CompletionException if an unrecoverable error occurs during the transaction execution that is not
     *                             related to retryable error code 1020.
     */
    private DirectorySubspace createOrOpenVolumeSubspace(KronotopDirectoryNode directory) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, directory.toList()).join();
            tr.commit().join();
            return subspace;
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                if (ex.getCode() == 1020) {
                    return createOrOpenVolumeSubspace(directory);
                }
            }
            throw e;
        }
    }

    private String volumeName(ShardKind shardKind, int shardId) {
        return String.format("%s-shard-%d", shardKind, shardId);
    }

    /**
     * Generates a new VolumeConfig for a Redis shard.
     *
     * @param subspace the DirectorySubspace associated with the volume configuration
     * @param dataDir  the directory path where the volume's data will be stored
     * @return a VolumeConfig object containing the configuration details for the Redis shard
     */
    private VolumeConfig newRedisShardVolumeConfig(DirectorySubspace subspace, String dataDir) {
        String name = volumeName(ShardKind.REDIS, shardId);
        Config config = context.getConfig().getConfig("redis.volume_syncer");
        long segmentSize = config.getLong("segment_size");
        float allowedGarbageRatio = (float) config.getDouble("allowed_garbage_ratio");

        return new VolumeConfig(subspace, name, dataDir, segmentSize, allowedGarbageRatio);
    }

    /**
     * Constructs the directory path for storing shard-related data.
     * <p>
     * The path is built using the base data directory from the context, the
     * lowercased name of the shard kind, a fixed folder name "shards", and the shard ID.
     *
     * @return the complete directory path as a String where shard data is to be stored.
     */
    public String getDataDir() {
        return Path.of(
                context.getDataDir().toString(),
                shardKind.name().toLowerCase(),
                "shards",
                Integer.toString(shardId)
        ).toString();
    }

    public VolumeConfig volumeConfig() {
        return volumeConfig(getDataDir());
    }

    /**
     * Generates a volume configuration for a Redis shard based on the provided data directory.
     *
     * @param dataDir the directory where data for the volume will be stored
     * @return a VolumeConfig object containing the configuration details for the Redis shard volume
     * @throws IllegalArgumentException if the shard kind is not recognized
     */
    public VolumeConfig volumeConfig(String dataDir) {
        if (shardKind.equals(ShardKind.REDIS)) {
            KronotopDirectoryNode directory = getRedisShardVolumeDirectory();
            DirectorySubspace subspace = createOrOpenVolumeSubspace(directory);
            return newRedisShardVolumeConfig(subspace, dataDir);
        } else {
            throw new IllegalArgumentException("Unknown shard kind: " + shardKind);
        }
    }

    /**
     * Creates or opens a DirectorySubspace for a volume associated with a Redis shard.
     * This method evaluates the shard kind and, if it is of kind REDIS, it retrieves the
     * corresponding KronotopDirectoryNode. It then calls an internal method to create or open
     * the DirectorySubspace based on the node's directory path.
     *
     * @return the DirectorySubspace corresponding to the volume associated with the Redis shard
     * @throws IllegalArgumentException if the shard kind is not recognized
     */
    public DirectorySubspace createOrOpenVolumeSubspace() {
        if (shardKind.equals(ShardKind.REDIS)) {
            KronotopDirectoryNode directory = getRedisShardVolumeDirectory();
            return createOrOpenVolumeSubspace(directory);
        } else {
            throw new IllegalArgumentException("Unknown shard kind: " + shardKind);
        }
    }
}
