// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.kronotop.Context;
import com.kronotop.ServiceContext;
import com.kronotop.internal.RoundRobin;

import java.util.List;

public class RoundRobinShardSelector implements ShardSelector {
    private final RoundRobin<BucketShard> scheduler = new RoundRobin<>(List.of());

    public RoundRobinShardSelector(Context context) {
        ServiceContext<BucketShard> serviceContext = context.getServiceContext(BucketService.NAME);
        for (BucketShard shard : serviceContext.shards().values()) {
            scheduler.add(shard);
        }
    }

    public BucketShard next() {
        return scheduler.next();
    }

    public void add(BucketShard shard) {
        scheduler.add(shard);
    }

    public void remove(BucketShard shard) {
        scheduler.remove(shard);
    }
}
