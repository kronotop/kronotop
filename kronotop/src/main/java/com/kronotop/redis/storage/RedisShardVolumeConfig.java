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

package com.kronotop.redis.storage;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.volume.VolumeConfig;
import com.typesafe.config.Config;

import java.nio.file.Path;
import java.util.concurrent.CompletionException;

public class RedisShardVolumeConfig {

    public static VolumeConfig newVolumeConfig(Context context, int sharId) {
        String dataDir = Path.of(
                context.getDataDir().toString(),
                "redis",
                "shards",
                Integer.toString(sharId)
        ).toString(); // $data_dir/redis/shards/$shard_number
        Config config = context.getConfig().getConfig("redis.volume_syncer");
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                redis().
                volume(Integer.toString(sharId));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, directory.toList()).join();
            VolumeConfig volumeConfig = new VolumeConfig(
                    subspace,
                    String.format("redis-shard-%d", sharId),
                    dataDir,
                    config.getLong("segment_size"),
                    (float) config.getDouble("allowed_garbage_ratio")
            );
            tr.commit().join();
            return volumeConfig;
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                if (ex.getCode() == 1020) {
                    return newVolumeConfig(context, sharId);
                }
            }
            throw e;
        }
    }
}
