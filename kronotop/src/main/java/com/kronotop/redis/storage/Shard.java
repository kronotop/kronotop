/*
 * Copyright (c) 2023 Kronotop
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
import com.kronotop.redis.storage.index.Index;
import com.kronotop.redis.storage.persistence.PersistenceQueue;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This interface represents a shard, which is a concurrent map with additional functionality.
 * It extends the ConcurrentMap interface to provide all the standard map operations.
 */
public interface Shard extends ConcurrentMap<String, Object> {
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
    Index getIndex();

    /**
     * @return the Striped object that provides access to a collection of ReadWriteLocks
     */
    Striped<ReadWriteLock> getStriped();

    /**
     * Retrieves the PersistenceQueue associated with the Shard.
     *
     * @return the PersistenceQueue associated with the Shard
     */
    PersistenceQueue getPersistenceQueue();

    /**
     * Retrieves the ID associated with this shard.
     *
     * @return the ID associated with this shard
     */
    Integer getId();

    /**
     * Checks if the shard is operable.
     *
     * @return true if the shard is operable, false otherwise
     */
    boolean isOperable();

    /**
     * Sets the operable flag for the shard.
     *
     * @param operable true to set the shard as operable, false otherwise
     */
    void setOperable(boolean operable);
}