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

package com.kronotop.redis.storage.index;

/**
 * Represents an index that supports operations such as adding, removing,
 * retrieving, and projecting keys. The interface provides thread-safe
 * interaction capabilities for managing keys efficiently and supports
 * features like flushing changes, retrieving a random key, and fetching
 * projections of keys based on specified parameters.
 */
public interface Index {
    /**
     * Adds the specified key to the index.
     *
     * @param key the key to be added to the index
     */
    void add(String key);

    /**
     * Removes the specified key from the index.
     *
     * @param key the key to be removed from the index
     */
    void remove(String key);

    /**
     * Returns the number of keys currently stored in the index.
     *
     * @return the total number of keys in the index
     */
    int size();

    /**
     * Retrieves the head position or identifier of the index.
     *
     * @return the head value of the index as a {@code Long}, or null if the index is empty
     */
    Long head();

    /**
     * Retrieves a random key from the index.
     *
     * @return a random key as a String, or null if the index is empty
     */
    String random();

    /**
     * Flushes the current state of the index by persisting or finalizing any pending changes.
     * This operation ensures that the index is in a consistent and stable state.
     */
    void flush();

    /**
     * Retrieves a projection from the index based on the specified offset and count.
     *
     * @param offset the starting position from which keys should be retrieved
     * @param count  the maximum number of keys to retrieve
     * @return a {@link Projection} object containing the keys retrieved and the next cursor position
     */
    Projection getProjection(long offset, int count);
}
