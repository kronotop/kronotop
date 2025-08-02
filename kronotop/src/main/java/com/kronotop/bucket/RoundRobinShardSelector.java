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

import com.kronotop.internal.RoundRobin;

import java.util.List;

/**
 * The {@code RoundRobinShardSelector} is a concrete implementation of the {@link ShardSelector} interface.
 * It utilizes a round-robin scheduling strategy to manage and distribute operations across multiple {@link BucketShard}
 * instances. The selector ensures fair and cyclic allocation of operations across the available shards.
 * <p>
 * This class leverages the {@link RoundRobin} utility to provide thread-safe iteration over the managed shards
 * and allows for dynamic addition or removal of shards at runtime.
 */
public class RoundRobinShardSelector implements ShardSelector {
    private final RoundRobin<BucketShard> scheduler = new RoundRobin<>(List.of());

    /**
     * Selects and retrieves the next {@link BucketShard} instance in a round-robin manner.
     * Ensures cyclic and fair distribution of requests across the available shards.
     *
     * @return the next {@link BucketShard} in the round-robin sequence.
     * @throws IllegalStateException if no {@link BucketShard} instances are available or if the
     *                               internal state of the round-robin scheduler is corrupted.
     */
    public BucketShard next() {
        return scheduler.next();
    }

    /**
     * Adds a {@link BucketShard} instance to the round-robin scheduler for inclusion in the selection process.
     *
     * @param shard the {@link BucketShard} instance to be added to the scheduler;
     *              must not already exist in the scheduler.
     * @throws IllegalStateException if the provided {@link BucketShard} instance already exists in the scheduler.
     */
    public void add(BucketShard shard) {
        scheduler.add(shard);
    }

    /**
     * Removes a {@link BucketShard} instance from the round-robin scheduler,
     * effectively excluding it from the selection process.
     *
     * @param shard the {@link BucketShard} instance to be removed from the scheduler;
     *              must exist in the scheduler.
     */
    public void remove(BucketShard shard) {
        scheduler.remove(shard);
    }
}
