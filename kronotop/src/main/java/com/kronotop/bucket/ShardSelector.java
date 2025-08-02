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

/**
 * An interface for determining the selection and management of {@link BucketShard} instances
 * for Bucket operations. The {@code ShardSelector} is responsible for distributing operations or load
 * across different {@code BucketShard} instances and allows adding or removing shards dynamically.
 */
public interface ShardSelector {
    /**
     * Selects the next {@code BucketShard} for performing operations in a round-robin
     * or other defined selection strategy.
     *
     * @return the next {@code BucketShard} to be used
     */
    BucketShard next();

    /**
     * Adds the given {@code BucketShard} to the collection of shard instances managed by this
     * {@code ShardSelector}. This allows the shard to participate in future operations or
     * load distribution managed by the selector.
     *
     * @param shard the {@code BucketShard} to be added to the collection of managed shards
     */
    void add(BucketShard shard);

    /**
     * Removes the specified {@code BucketShard} from the collection of shard instances
     * managed by this {@code ShardSelector}. Once removed, the shard will no longer
     * participate in future operations or load distribution managed by the selector.
     *
     * @param shard the {@code BucketShard} to be removed from the collection of managed shards
     */
    void remove(BucketShard shard);
}
