/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.index;

import com.kronotop.internal.UUIDUtil;

import java.util.UUID;

/**
 * Represents an immutable vector index definition for HNSW-based approximate nearest neighbor search.
 *
 * @param id         unique identifier generated from UUID hash using SipHash24 algorithm
 * @param name       a human-readable index name must be unique within a bucket
 * @param selector   document field path containing the vector data
 * @param dimensions number of dimensions in the vector
 * @param distance   distance function used for similarity computation
 * @param status     current operational status of the index
 */
public record VectorIndexDefinition(long id, String name, String selector, int dimensions,
                                    DistanceFunction distance,
                                    IndexStatus status) implements IndexDefinition {

    /**
     * Creates a new vector index definition with the specified status.
     *
     * @param name       the human-readable name for the index
     * @param selector   the document field path containing the vector data
     * @param dimensions the number of dimensions
     * @param distance   the distance function for similarity computation
     * @param status     the initial status assigned to the index
     * @return a new VectorIndexDefinition instance
     */
    public static VectorIndexDefinition create(String name, String selector, int dimensions, DistanceFunction distance, IndexStatus status) {
        if (dimensions < 1) {
            throw new IllegalArgumentException("Vector index dimensions must be >= 1, got " + dimensions);
        }
        UUID uuid = UUID.randomUUID();
        long id = UUIDUtil.hash(uuid).asLong();
        return new VectorIndexDefinition(id, name, selector, dimensions, distance, status);
    }

    @Override
    public VectorIndexDefinition updateStatus(IndexStatus status) {
        if (status != IndexStatus.DROPPED && status() == IndexStatus.DROPPED) {
            throw new IllegalStateException("Index '" + name + "' is already dropped and its status cannot be modified.");
        }
        return new VectorIndexDefinition(id, name, selector, dimensions, distance, status);
    }
}
