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
import com.kronotop.redis.handlers.hash.protocol.HGetAllMessage;
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
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.redis.RedisService.checkRedisValueKind;

@Command(HGetAllMessage.COMMAND)
@MinimumParameterCount(HGetAllMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HGetAllMessage.MAXIMUM_PARAMETER_COUNT)
public class HGetAllHandler extends BaseHandler implements Handler {
    public HGetAllHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HGETALL).set(new HGetAllMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HGetAllMessage hgetallMessage = request.attr(MessageTypes.HGETALL).get();

        List<RedisMessage> result = new ArrayList<>();
        RedisShard shard = service.findShard(hgetallMessage.getKey(), ShardStatus.READONLY);
        ReadWriteLock lock = shard.striped().get(hgetallMessage.getKey());
        lock.readLock().lock();
        try {
            RedisValueContainer container = shard.storage().get(hgetallMessage.getKey());
            if (container == null) {
                response.writeArray(result);
                return;
            }
            checkRedisValueKind(container, RedisValueKind.HASH);

            Enumeration<String> fields = container.hash().keys();
            while (fields.hasMoreElements()) {
                String field = fields.nextElement();
                ByteBuf fieldBuf = response.getChannelContext().alloc().buffer();
                fieldBuf.writeBytes(field.getBytes());
                result.add(new FullBulkStringRedisMessage(fieldBuf));

                HashFieldValue hashField = container.hash().get(field);
                ByteBuf valueBuf = response.getChannelContext().alloc().buffer();
                valueBuf.writeBytes(hashField.value());
                result.add(new FullBulkStringRedisMessage(valueBuf));
            }
        } finally {
            lock.readLock().unlock();
        }
        response.writeArray(result);
    }
}