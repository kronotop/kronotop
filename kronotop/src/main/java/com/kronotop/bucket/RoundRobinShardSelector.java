// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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
