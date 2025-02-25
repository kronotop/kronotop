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

package com.kronotop.redis.handlers.hash;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.handlers.hash.protocol.HGetMessage;
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
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.redis.RedisService.checkRedisValueKind;

@Command(HGetMessage.COMMAND)
@MinimumParameterCount(HGetMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HGetMessage.MAXIMUM_PARAMETER_COUNT)
public class HGetHandler extends BaseHandler implements Handler {
    public HGetHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HGET).set(new HGetMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HGetMessage hgetMessage = request.attr(MessageTypes.HGET).get();

        RedisShard shard = service.findShard(hgetMessage.getKey(), ShardStatus.READONLY);
        ReadWriteLock lock = shard.striped().get(hgetMessage.getKey());
        lock.readLock().lock();
        try {
            RedisValueContainer container = shard.storage().get(hgetMessage.getKey());
            if (container == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }
            checkRedisValueKind(container, RedisValueKind.HASH);

            HashFieldValue hashField = container.hash().get(hgetMessage.getField());
            if (hashField == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }

            ByteBuf buf = response.getCtx().alloc().buffer();
            buf.writeBytes(hashField.value());
            response.write(buf);
        } finally {
            lock.readLock().unlock();
        }
    }
}
