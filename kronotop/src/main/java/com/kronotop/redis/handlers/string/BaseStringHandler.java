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

package com.kronotop.redis.handlers.string;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.syncer.jobs.AppendStringJob;
import com.kronotop.redis.storage.syncer.jobs.DeleteByVersionstampJob;

public class BaseStringHandler extends BaseHandler {
    public BaseStringHandler(RedisService service) {
        super(service);
    }

    protected void deleteByVersionstamp(RedisShard shard, Versionstamp versionstamp) {
        if (versionstamp != null) {
            shard.volumeSyncQueue().add(new DeleteByVersionstampJob(versionstamp));
        }
    }

    protected void syncMutatedStringOnVolume(RedisShard shard, String key, Versionstamp versionstamp) {
        shard.volumeSyncQueue().add(new AppendStringJob(key));
        if (versionstamp != null) {
            shard.volumeSyncQueue().add(new DeleteByVersionstampJob(versionstamp));
        }
    }

    protected void syncStringOnVolume(RedisShard shard, String key, RedisValueContainer previous) {
        if (previous == null) {
            shard.volumeSyncQueue().add(new AppendStringJob(key));
            shard.index().add(key);
        } else {
            shard.volumeSyncQueue().add(new AppendStringJob(key));
            Versionstamp versionstamp = previous.baseRedisValue().versionstamp();
            if (versionstamp != null) {
                shard.volumeSyncQueue().add(new DeleteByVersionstampJob(versionstamp));
            }
        }
    }

    /**
     * Evicts a string from the Redis shard if its time-to-live (TTL) has expired.
     * <p>
     * This method checks whether the TTL of the given string has expired by comparing it
     * against the current system time. If the TTL has expired, the string is removed
     * from the shard's storage and index. Additionally, if the removed string has an
     * associated versionstamp, a deletion job is added to the shard's volume sync queue for synchronization.
     *
     * @param container the RedisValueContainer holding the string value that is subject to eviction
     * @param shard     the RedisShard from which the string value may be evicted
     * @param key       the key to identifying the Redis entry to potentially evict
     * @return true if the string was evicted due to an expired TTL, false otherwise
     */
    protected boolean evictStringIfNeeded(RedisValueContainer container, RedisShard shard, String key) {
        if (container.string().ttl() == 0) {
            // no associated TTL
            return false;
        }

        if (container.string().ttl() <= service.getCurrentTimeInMilliseconds()) {
            RedisValueContainer previous = shard.storage().remove(key);
            shard.index().remove(key);
            if (previous.baseRedisValue().versionstamp() != null) {
                shard.volumeSyncQueue().add(new DeleteByVersionstampJob(previous.baseRedisValue().versionstamp()));
            }
            return true;
        }
        return false;
    }
}
