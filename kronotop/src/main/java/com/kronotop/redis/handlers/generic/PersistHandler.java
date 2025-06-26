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

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.generic.protocol.PersistMessage;
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

@Command(PersistMessage.COMMAND)
@MaximumParameterCount(PersistMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(PersistMessage.MINIMUM_PARAMETER_COUNT)
public class PersistHandler extends BaseGenericHandler implements Handler {
    public PersistHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.PERSIST).set(new PersistMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        PersistMessage message = request.attr(MessageTypes.PERSIST).get();

        RedisShard shard = service.findShard(message.getKey(), ShardStatus.READONLY);
        ReadWriteLock lock = shard.striped().get(message.getKey());
        try {
            lock.writeLock().lock();
            RedisValueContainer container = shard.storage().get(message.getKey());
            if (container == null) {
                // Integer reply: 0 if key does not exist or does not have an associated timeout.
                response.writeInteger(0);
                return;
            }

            if (!container.kind().equals(RedisValueKind.STRING)) {
                // Currently, only STRING supports the TTLs in Kronotop.
                response.writeInteger(0);
                return;
            }

            if (container.string().ttl() == 0) {
                // Integer reply: 0 if key does not exist or does not have an associated timeout.
                response.writeInteger(0);
            }
            container.string().setTTL(0);
            RedisValueContainer previous = shard.storage().put(message.getKey(), container);
            syncStringOnVolume(shard, message.getKey(), previous);
            response.writeInteger(1);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
