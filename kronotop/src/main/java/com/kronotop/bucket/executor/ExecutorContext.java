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

public record ExecutorContext(BucketShard shard, PhysicalNode plan, String bucket, BucketSubspace subspace) {
}
