// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.kronotop.Context;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.impl.AbstractShard;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeAttributes;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeConfigGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;

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
    }

    @Override
    public Volume volume() {
        return volume;
    }

    @Override
    public void close() {
        volume.close();
    }
}
