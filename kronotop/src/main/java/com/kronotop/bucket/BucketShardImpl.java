/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket;

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceWatchDog;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.impl.AbstractShard;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeAttributes;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeConfigGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the {@link BucketShard} interface, representing a specific type of shard
 * (Bucket Shard) that is associated with a unique {@link Volume}.
 *
 * <p>This class extends {@link AbstractShard}, inheriting common shard functionalities,
 * and implements specific behaviors for bucket-based shard interactions, such as handling
 * volume configurations and resources.
 *
 * <p>A {@code BucketShardImpl} is initialized with a {@link Context} and a unique identifier.
 * During initialization, it sets up a {@link Volume} instance associated with the shard and
 * assigns relevant attributes such as the shard ID and kind.
 *
 * <p>The class ensures proper exception handling during the volume creation process by wrapping
 * {@link IOException} with {@link UncheckedIOException} to simplify error management.
 *
 * <p>The {@code close()} method is implemented to release the resources associated with
 * the volume when the shard is no longer needed.
 *
 * @see AbstractShard
 * @see BucketShard
 * @see Volume
 */
public class BucketShardImpl extends AbstractShard implements BucketShard {
    private final Volume volume;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final IndexMaintenanceWatchDog worker;
    private volatile boolean closed;

    public BucketShardImpl(Context context, int id) {
        super(context, ShardKind.BUCKET, id);

        VolumeConfig volumeConfig = new VolumeConfigGenerator(context, ShardKind.BUCKET, id).volumeConfig();
        try {
            Volume volume = volumeService.newVolume(volumeConfig);
            volume.setAttribute(VolumeAttributes.SHARD_ID, id);
            volume.setAttribute(VolumeAttributes.SHARD_KIND, ShardKind.BUCKET);
            this.volume = volume;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.worker = new IndexMaintenanceWatchDog(context, this);
        executor.submit(worker);
    }

    @Override
    public Volume volume() {
        return volume;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
        volume.close();
        worker.shutdown();
        executor.shutdownNow();
        ExecutorServiceUtil.shutdownNowThenAwaitTermination(executor);
    }
}
