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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.Context;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.transaction.TransactionUtil;
import io.github.resilience4j.retry.Retry;

import java.nio.file.Path;
import java.time.Duration;
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

    private KronotopDirectoryNode getBucketShardVolumeDirectory() {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                bucket().
                volume(Integer.toString(shardId));
    }

    /**
     * Creates or opens a DirectorySubspace for a specified directory within the Kronotop structure.
     * This method attempts to create or retrieve the subspace for the given directory,
     * optionally creating it if it does not exist, based on the provided flag.
     * <p>
     * If a FoundationDB transaction conflict is detected (error code 1020), the method will retry.
     * Any other exceptions are propagated to the caller.
     *
     * @param directory        the KronotopDirectoryNode representing the directory path for the subspace.
     * @param createIfNotExist a boolean flag indicating whether to create the DirectorySubspace if it doesn't exist.
     *                         If false, the method attempts to open the subspace, failing if it does not exist.
     * @return the created or opened DirectorySubspace associated with the specified Kronotop directory.
     * @throws CompletionException if the creation or opening of the subspace fails for any reason other than a
     *                             transaction conflict (which is retried automatically).
     */
    private DirectorySubspace createOrOpenVolumeSubspace(KronotopDirectoryNode directory, boolean createIfNotExist) {
        Retry retry = TransactionUtil.retry(10, Duration.ofMillis(100));
        return retry.executeSupplier(() -> {
            try (Transaction tr = context.getTransactionManager().create()) {
                DirectorySubspace subspace;
                if (createIfNotExist) {
                    subspace = context.getDirectoryLayer().createOrOpen(tr, directory.toList()).join();
                } else {
                    subspace = context.getDirectoryLayer().open(tr, directory.toList()).join();
                }
                tr.commit().join();
                return subspace;
            } catch (CompletionException exp) {
                if (exp.getCause() instanceof NoSuchDirectoryException) {
                    throw new IllegalArgumentException("No such volume");
                }
                throw exp;
            }
        });
    }

    private VolumeConfig newBucketShardVolumeConfig(DirectorySubspace subspace, String dataDir) {
        String name = VolumeNames.format(ShardKind.BUCKET, shardId);
        long segmentSize = context.getConfig().getLong("bucket.volume.segment_size");
        return new VolumeConfig(subspace, name, dataDir, segmentSize);
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
     * Generates a volume configuration for a Bucket shard based on the provided data directory.
     *
     * @param dataDir the directory where data for the volume will be stored
     * @return a VolumeConfig object containing the configuration details for the Bucket shard volume
     * @throws IllegalArgumentException if the shard kind is not recognized
     */
    public VolumeConfig volumeConfig(String dataDir) {
        if (shardKind.equals(ShardKind.BUCKET)) {
            KronotopDirectoryNode directory = getBucketShardVolumeDirectory();
            DirectorySubspace subspace = createOrOpenVolumeSubspace(directory, true);
            return newBucketShardVolumeConfig(subspace, dataDir);
        } else {
            throw new IllegalArgumentException("Unknown shard kind: " + shardKind);
        }
    }

    /**
     * Creates or opens the DirectorySubspace for the volume of a Bucket shard.
     *
     * @return the DirectorySubspace corresponding to the volume associated with the Bucket shard
     * @throws IllegalArgumentException if the shard kind is not recognized
     */
    public DirectorySubspace createOrOpenVolumeSubspace() {
        if (shardKind.equals(ShardKind.BUCKET)) {
            KronotopDirectoryNode directory = getBucketShardVolumeDirectory();
            return createOrOpenVolumeSubspace(directory, true);
        } else {
            throw new IllegalArgumentException("Unknown shard kind: " + shardKind);
        }
    }

    /**
     * Opens the DirectorySubspace associated with a volume's configuration.
     *
     * @return the DirectorySubspace corresponding to the volume configuration of the Bucket shard
     * @throws IllegalArgumentException if the shard kind is unrecognized
     */
    public DirectorySubspace openVolumeSubspace() {
        if (shardKind.equals(ShardKind.BUCKET)) {
            KronotopDirectoryNode directory = getBucketShardVolumeDirectory();
            return createOrOpenVolumeSubspace(directory, false);
        } else {
            throw new IllegalArgumentException("Unknown shard kind: " + shardKind);
        }
    }
}
