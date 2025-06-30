// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.executor;

import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.BucketSubspace;
import com.kronotop.bucket.planner.physical.PhysicalNode;

/**
 * ExecutorContext represents the execution context for a specific operation
 * within a distributed storage system. It holds references to the bucket shard,
 * physical execution plan, bucket identifier, bucket subspace, and associated indexes.
 * <p>
 * This class is immutable, with all fields set during construction.
 */
public class PlanExecutorEnvironment {
    private final BucketShard shard;
    private final PhysicalNode plan;
    private final String bucket;
    private final BucketSubspace subspace;

    // Add mutable fields here

    public PlanExecutorEnvironment(
            BucketShard shard,
            PhysicalNode plan,
            String bucket,
            BucketSubspace subspace,
    ) {
        this.shard = shard;
        this.plan = plan;
        this.bucket = bucket;
        this.subspace = subspace;
    }

    public BucketShard shard() {
        return shard;
    }

    public PhysicalNode plan() {
        return plan;
    }

    public String bucket() {
        return bucket;
    }

    public BucketSubspace subspace() {
        return subspace;
    }
}
