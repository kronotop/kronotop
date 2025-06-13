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

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.string.protocol.TTLMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.RedisValueKind;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.concurrent.locks.ReadWriteLock;

@Command(TTLMessage.COMMAND)
@MaximumParameterCount(TTLMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(TTLMessage.MINIMUM_PARAMETER_COUNT)
public class TTLHandler extends BaseStringHandler implements Handler {
    public TTLHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.TTL).set(new TTLMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        TTLMessage ttlMessage = request.attr(MessageTypes.TTL).get();

        RedisShard shard = service.findShard(ttlMessage.getKey(), ShardStatus.READONLY);
        ReadWriteLock lock = shard.striped().get(ttlMessage.getKey());
        try {
            lock.readLock().lock();
            RedisValueContainer container = shard.storage().get(ttlMessage.getKey());
            if (container == null) {
                // The command returns -2 if the key does not exist.
                response.writeInteger(-2);
                return;
            }
            if (!container.kind().equals(RedisValueKind.STRING)) {
                // Only STRING supports the TTLs in Kronotop.
                response.writeInteger(-1);
                return;
            }
            if (container.string().ttl() == 0) {
                // The command returns -1 if the key exists but has no associated expire.
                response.writeInteger(-1);
                return;
            }
            response.writeInteger(container.string().ttl());
        } finally {
            lock.readLock().unlock();
        }
    }
}
