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
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.transaction.TransactionUtil;
import io.github.resilience4j.retry.Retry;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletionException;

/**
 * Builds the volume configuration of a shard. It resolves the local data directory of the shard
 * and the FoundationDB directory subspace of its volume.
 */
public class VolumeConfigGenerator {
    private final Context context;
    private final int shardId;

    /**
     * Creates a generator for the volume of the given shard.
     *
     * @param context the context of the member
     * @param shardId the shard ID
     */
    public VolumeConfigGenerator(Context context, int shardId) {
        this.context = context;
        this.shardId = shardId;
    }

    private KronotopDirectoryNode getShardVolumeDirectory() {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                volume(Integer.toString(shardId));
    }

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

    private VolumeConfig newShardVolumeConfig(DirectorySubspace subspace, String dataDir) {
        String name = VolumeNames.format(shardId);
        long segmentSize = context.getConfig().getLong("volume.segment_size");
        return new VolumeConfig(subspace, name, dataDir, segmentSize);
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

    public String getDataDir() {
        return shardDataDir(context.getDataDir(), shardId).toString();
    }

    /**
     * Returns the volume configuration of the shard with its default local directory.
     *
     * @return the configuration of the shard volume
     */
    public VolumeConfig volumeConfig() {
        return volumeConfig(getDataDir());
    }

    /**
     * Returns the volume configuration of the shard with the given local directory. Creates the
     * directory subspace of the volume if it does not exist.
     *
     * @param dataDir the local directory that stores the volume files
     * @return the configuration of the shard volume
     */
    public VolumeConfig volumeConfig(String dataDir) {
        DirectorySubspace subspace = createOrOpenVolumeSubspace(getShardVolumeDirectory(), true);
        return newShardVolumeConfig(subspace, dataDir);
    }

    /**
     * Opens the directory subspace of the shard volume. Creates it if it does not exist.
     *
     * @return the directory subspace of the shard volume
     */
    public DirectorySubspace createOrOpenVolumeSubspace() {
        return createOrOpenVolumeSubspace(getShardVolumeDirectory(), true);
    }

    /**
     * Opens the directory subspace of the shard volume.
     *
     * @return the directory subspace of the shard volume
     * @throws IllegalArgumentException if the volume does not exist
     */
    public DirectorySubspace openVolumeSubspace() {
        return createOrOpenVolumeSubspace(getShardVolumeDirectory(), false);
    }
}
