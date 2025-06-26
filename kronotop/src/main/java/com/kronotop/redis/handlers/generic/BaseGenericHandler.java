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

package com.kronotop.redis.handlers.generic;

import com.kronotop.KronotopException;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.generic.protocol.ExpireMessage;
import com.kronotop.redis.handlers.string.BaseStringHandler;
import com.kronotop.redis.handlers.string.StringValue;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.RedisValueKind;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;

public class BaseGenericHandler extends BaseStringHandler {
    public BaseGenericHandler(RedisService service) {
        super(service);
    }

    protected void wipeOutKey(RedisShard shard, String key, RedisValueContainer previous) {
        shard.index().remove(key);
        deleteByVersionstamp(shard, previous.baseRedisValue().versionstamp());
    }

    /**
     * Sets an expiration timeout on a key in the Redis system. This method is used to apply
     * a time-to-live (TTL) value to a key, ensuring that it will expire after the specified time.
     * If the key does not exist or arguments are invalid, the method will return without applying any changes.
     *
     * @param key    the key for which the expiration timeout is to be set
     * @param ttl    the time-to-live for the key in milliseconds
     * @param option additional options to customize the expiration logic, such as NX, XX, GT, or LT
     * @return 1 if the timeout was successfully set, 0 if the key does not exist or the timeout cannot be applied
     * @throws KronotopException if the operation encounters an error, like unsupported data structure types
     */
    protected long expireCommon(String key, long ttl, ExpireMessage.Option option) {
        // A non-volatile key is treated as an infinite TTL for the purpose of GT and LT. The GT, LT
        // and NX options are mutually exclusive.
        RedisShard shard = service.findShard(key, ShardStatus.READWRITE);
        ReadWriteLock lock = shard.striped().get(key);
        lock.writeLock().lock();
        try {
            RedisValueContainer current = shard.storage().get(key);
            if (current == null) {
                // Integer reply: 0 if the timeout was not set; for example, the key doesn't exist, or the operation was
                // skipped because of the provided arguments.
                return 0;
            }
            // Only supports STRING data structure
            if (!current.kind().equals(RedisValueKind.STRING)) {
                throw new KronotopException("EXPIRE command only supports string values");
            }

            if (Objects.nonNull(option)) {
                if (option.equals(ExpireMessage.Option.NX)) {
                    // NX -- Set expiry only when the key has no expiry
                    if (current.string().ttl() > 0) {
                        return 0;
                    }
                } else if (option.equals(ExpireMessage.Option.XX)) {
                    // XX -- Set expiry only when the key has an existing expiry
                    if (current.string().ttl() == 0) {
                        return 0;
                    }
                } else if (option.equals(ExpireMessage.Option.GT)) {
                    if (current.string().ttl() == 0) {
                        return 0;
                    }
                    // GT -- Set expiry only when the new expiry is greater than current one
                    if (ttl <= current.string().ttl()) {
                        return 0;
                    }
                } else if (option.equals(ExpireMessage.Option.LT)) {
                    if (current.string().ttl() == 0) {
                        return 0;
                    }
                    // LT -- Set expiry only when the new expiry is less than current one
                    if (ttl >= current.string().ttl()) {
                        return 0;
                    }
                }
            }

            StringValue stringValue = new StringValue(current.string().value());
            RedisValueContainer container = new RedisValueContainer(stringValue);
            container.string().setTTL(ttl);
            shard.storage().put(key, container);

            syncStringOnVolume(shard, key, current);
        } finally {
            lock.writeLock().unlock();
        }
        return 1;
    }
}
