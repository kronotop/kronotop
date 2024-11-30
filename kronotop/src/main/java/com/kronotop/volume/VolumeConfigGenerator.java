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

public class VolumeConfigGenerator {
    private final Context context;
    private final ShardKind shardKind;
    private final int shardId;

    public VolumeConfigGenerator(Context context, ShardKind shardKind, int shardId) {
        this.context = context;
        this.shardKind = shardKind;
        this.shardId = shardId;
    }

    private KronotopDirectoryNode getRedisShardVolumeDirectory() {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                redis().
                volume(Integer.toString(shardId));
    }

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

    private VolumeConfig newRedisShardVolumeConfig(DirectorySubspace subspace) {
        String name = String.format("redis-shard-%d", shardId);

        String dataDir = Path.of(
                context.getDataDir().toString(),
                ShardKind.REDIS.name().toLowerCase(),
                "shards",
                Integer.toString(shardId)
        ).toString();

        Config config = context.getConfig().getConfig("redis.volume_syncer");
        long segmentSize = config.getLong("segment_size");
        float allowedGarbageRatio = (float) config.getDouble("allowed_garbage_ratio");

        return new VolumeConfig(subspace, name, dataDir, segmentSize, allowedGarbageRatio);
    }

    public VolumeConfig volumeConfig() {
        if (shardKind.equals(ShardKind.REDIS)) {
            KronotopDirectoryNode directory = getRedisShardVolumeDirectory();
            DirectorySubspace subspace = createOrOpenVolumeSubspace(directory);
            return newRedisShardVolumeConfig(subspace);
        } else {
            throw new IllegalArgumentException("Unknown shard kind: " + shardKind);
        }
    }

    public DirectorySubspace createOrOpenVolumeSubspace() {
        if (shardKind.equals(ShardKind.REDIS)) {
            KronotopDirectoryNode directory = getRedisShardVolumeDirectory();
            return createOrOpenVolumeSubspace(directory);
        } else {
            throw new IllegalArgumentException("Unknown shard kind: " + shardKind);
        }
    }
}
