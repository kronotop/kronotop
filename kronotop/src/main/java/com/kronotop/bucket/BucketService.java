// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.bucket.handlers.BucketInsertHandler;
import com.kronotop.server.ServerKind;
import com.kronotop.volume.Prefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class BucketService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Bucket";
    protected static final Logger LOGGER = LoggerFactory.getLogger(BucketService.class);
    private final PrefixCache prefixCache;
    private final ConcurrentHashMap<Integer, BucketShard> shards = new ConcurrentHashMap<>();

    public BucketService(Context context) {
        super(context, NAME);

        int numberOfShards = context.getConfig().getInt("bucket.shards");
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            BucketShard shard = new AbstractBucketShard(context, shardId);
            shards.put(shardId, shard);
        }

        prefixCache = new PrefixCache(context);

        handlerMethod(ServerKind.EXTERNAL, new BucketInsertHandler(this));
    }

    public BucketShard getShard(int shardId) {
        return shards.get(shardId);
    }

    public Prefix getPrefix(String namespace, String bucket) {
        return prefixCache.get(namespace, bucket);
    }

    public void start() {

    }
}
