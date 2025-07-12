// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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
