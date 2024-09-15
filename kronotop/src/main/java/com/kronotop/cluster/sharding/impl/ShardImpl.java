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

import com.kronotop.Context;
import com.kronotop.cluster.sharding.Shard;
import com.kronotop.cluster.sharding.ShardLocator;
import com.kronotop.cluster.sharding.ShardStatistics;
import com.kronotop.volume.VolumeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardImpl implements Shard {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardImpl.class);
    protected final Context context;
    protected final VolumeService volumeService;
    protected final int id;

    public ShardImpl(Context context, int id) {
        this.context = context;
        this.volumeService = context.getService(VolumeService.NAME);
        this.id = id;
    }

    /**
     * Retrieves the ID associated with this shard.
     *
     * @return the ID associated with this shard
     */
    @Override
    public Integer id() {
        return id;
    }

    @Override
    public ShardLocator getShardLocator() {
        return null;
    }

    @Override
    public ShardStatistics getShardStatistics() {
        return null;
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
    }
}
