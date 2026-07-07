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

package com.kronotop.bucket;

import com.kronotop.internal.RoundRobin;

import java.util.List;

/**
 * A {@link ShardSelector} that distributes operations across {@link BucketShard} instances in
 * round-robin order. It uses the {@link RoundRobin} utility for thread-safe iteration and supports
 * adding or removing shards at runtime.
 */
public class RoundRobinShardSelector implements ShardSelector {
    private final RoundRobin<BucketShard> scheduler = new RoundRobin<>(List.of());

    /**
     * Returns the next {@link BucketShard} in round-robin order.
     *
     * @return the next {@link BucketShard} in the round-robin sequence.
     * @throws IllegalStateException if no {@link BucketShard} instances are available.
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
