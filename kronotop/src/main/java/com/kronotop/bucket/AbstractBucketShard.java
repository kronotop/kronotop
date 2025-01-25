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
import com.kronotop.cluster.sharding.impl.ShardImpl;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeConfigGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;

public class AbstractBucketShard extends ShardImpl implements BucketShard {
    private final Volume volume;

    public AbstractBucketShard(Context context, int id) {
        super(context, ShardKind.BUCKET, id);

        VolumeConfig volumeConfig = new VolumeConfigGenerator(context, ShardKind.BUCKET, id).volumeConfig();
        try {
            this.volume = volumeService.newVolume(volumeConfig);
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
