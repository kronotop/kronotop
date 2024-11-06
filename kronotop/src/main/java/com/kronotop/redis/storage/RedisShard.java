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

import com.google.common.util.concurrent.Striped;
import com.kronotop.cluster.sharding.Shard;
import com.kronotop.redis.storage.index.Index;
import com.kronotop.redis.storage.syncer.VolumeSyncQueue;
import com.kronotop.volume.Volume;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This interface represents a shard, which is a concurrent map with additional functionality.
 * It extends the ConcurrentMap interface to provide all the standard map operations.
 */
public interface RedisShard extends Shard {

    /**
     * Provides access to the storage map which is used to hold Redis values.
     *
     * @return a ConcurrentMap where keys are strings representing Redis keys and values
     * are RedisValueContainer objects representing the stored Redis values.
     */
    ConcurrentMap<String, RedisValueContainer> storage();

    /**
     * Checks if the shard is read-only.
     *
     * @return true if the shard is read-only, false otherwise
     */
    boolean isReadOnly();

    /**
     * Sets the read-only flag for the shard.
     *
     * @param readOnly true to set the shard as read-only, false otherwise
     */
    void setReadOnly(boolean readOnly);

    /**
     * Returns the index associated with this shard.
     *
     * @return the index associated with this shard
     */
    Index index();

    /**
     * @return the Striped object that provides access to a collection of ReadWriteLocks
     */
    Striped<ReadWriteLock> striped();

    /**
     * Retrieves the append-only queue associated with this shard.
     * The append queue is utilized for synchronizing volume-related operations.
     *
     * @return the VolumeSyncQueue for this shard
     */
    VolumeSyncQueue volumeSyncQueue();

    /**
     * Retrieves the Volume associated with the Shard.
     *
     * @return the Volume associated with the Shard
     */
    Volume volume();

    /**
     * Closes the shard and frees allocated resources.
     */
    void close();
}