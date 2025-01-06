/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.redis.handlers.hash;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.handlers.hash.protocol.HLenMessage;
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

@Command(HLenMessage.COMMAND)
@MinimumParameterCount(HLenMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HLenMessage.MAXIMUM_PARAMETER_COUNT)
public class HLenHandler extends BaseHandler implements Handler {
    public HLenHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HLEN).set(new HLenMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HLenMessage hlenMessage = request.attr(MessageTypes.HLEN).get();

        RedisShard shard = service.findShard(hlenMessage.getKey(), ShardStatus.READONLY);
        ReadWriteLock lock = shard.striped().get(hlenMessage.getKey());
        lock.readLock().lock();
        try {
            RedisValueContainer container = shard.storage().get(hlenMessage.getKey());
            if (container == null) {
                response.writeInteger(0);
                return;
            }
            checkRedisValueKind(container, RedisValueKind.HASH);
            response.writeInteger(container.hash().size());
        } finally {
            lock.readLock().unlock();
        }
    }
}
