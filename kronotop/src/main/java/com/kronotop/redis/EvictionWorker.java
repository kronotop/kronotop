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

package com.kronotop.redis;

import com.kronotop.Context;
import com.kronotop.ServiceContext;
import com.kronotop.redis.storage.RedisShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Phaser;

/*
Periodically Redis tests a few keys at random among keys with an expire set. All the keys that are already expired are deleted from the keyspace.

Specifically this is what Redis does 10 times per second:

Test 20 random keys from the set of keys with an associated expire.
Delete all the keys found expired.
If more than 25% of keys were expired, start again from step 1.
This is a trivial probabilistic algorithm, basically the assumption is that our sample is representative of the whole key space, and we continue to expire until the percentage of keys that are likely to be expired is under 25%
 */


public class EvictionWorker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EvictionWorker.class);
    private final ServiceContext<RedisShard> serviceContext;

    public EvictionWorker(Context context) {
        this.serviceContext = context.getServiceContext(RedisService.NAME);
    }

    @Override
    public void run() {
        Phaser phaser = new Phaser(1);
        for (RedisShard shard : serviceContext.shards().values()) {
            ShardEvictionWorker worker = new ShardEvictionWorker(shard, phaser);
            phaser.register();
            serviceContext.root().getVirtualThreadPerTaskExecutor().submit(worker);
        }

        phaser.arriveAndAwaitAdvance();
        LOGGER.debug("EvictionWorker finished");
    }

    static class ShardEvictionWorker implements Runnable {
        private final RedisShard shard;
        private final Phaser phaser;

        public ShardEvictionWorker(RedisShard shard, Phaser phaser) {
            this.shard = shard;
            this.phaser = phaser;
        }

        @Override
        public void run() {
            try {
                LOGGER.debug("EvictionWorker started for shard {}", shard.id());
            } finally {
                phaser.arriveAndDeregister();
            }
        }
    }
}
