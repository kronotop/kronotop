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

package com.kronotop.cluster.sharding.impl;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.sharding.Shard;
import com.kronotop.volume.BaseVolumeTest;
import com.kronotop.volume.VolumeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

class ShardImplTest extends BaseVolumeTest {
    protected Shard shard;
    protected VolumeConfig volumeConfig;

    @BeforeEach
    public void setUp() {
        DirectorySubspace subspace = getSubspace(database, config);
        volumeConfig = getVolumeConfig(config, subspace);
        shard = new ShardImpl(context, 1);
    }

    @AfterEach
    public void tearDown() {
        shard.shutdown();
    }
}