/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.redis.hash;

import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.hash.protocol.HExistsMessage;
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

import static com.kronotop.redis.RedisService.checkRedisValueKind;

@Command(HExistsMessage.COMMAND)
@MinimumParameterCount(HExistsMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HExistsMessage.MAXIMUM_PARAMETER_COUNT)
public class HExistsHandler extends BaseHandler implements Handler {
    public HExistsHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HEXISTS).set(new HExistsMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HExistsMessage hexistsMessage = request.attr(MessageTypes.HEXISTS).get();

        RedisShard shard = service.findShard(hexistsMessage.getKey());
        ReadWriteLock lock = shard.striped().get(hexistsMessage.getKey());
        lock.readLock().lock();
        try {
            RedisValueContainer container = shard.storage().get(hexistsMessage.getKey());
            if (container == null) {
                response.writeInteger(0);
                return;
            }
            checkRedisValueKind(container, RedisValueKind.HASH);

            boolean exists = container.hash().containsKey(hexistsMessage.getField());
            if (exists) {
                response.writeInteger(1);
            } else {
                response.writeInteger(0);
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}
