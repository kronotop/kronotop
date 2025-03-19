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

import java.util.concurrent.ConcurrentHashMap;

public class BucketContext implements ServiceContext<BucketShard> {
    private final Context context;
    private final ConcurrentHashMap<Integer, BucketShard> shards = new ConcurrentHashMap<>();

    public BucketContext(Context context) {
        this.context = context;
    }

    @Override
    public ConcurrentHashMap<Integer, BucketShard> shards() {
        return shards;
    }

    @Override
    public Context root() {
        return context;
    }
}