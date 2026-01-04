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
import com.kronotop.redis.handlers.hash.protocol.HKeysMessage;
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
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.redis.RedisService.checkRedisValueKind;

@Command(HKeysMessage.COMMAND)
@MinimumParameterCount(HKeysMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HKeysMessage.MAXIMUM_PARAMETER_COUNT)
public class HKeysHandler extends BaseHandler implements Handler {
    public HKeysHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HKEYS).set(new HKeysMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HKeysMessage hkeysMessage = request.attr(MessageTypes.HKEYS).get();

        List<RedisMessage> fields = new ArrayList<>();
        RedisShard shard = service.findShard(hkeysMessage.getKey(), ShardStatus.READONLY);
        ReadWriteLock lock = shard.striped().get(hkeysMessage.getKey());
        lock.readLock().lock();
        try {
            RedisValueContainer container = shard.storage().get(hkeysMessage.getKey());
            if (container == null) {
                response.writeArray(fields);
                return;
            }
            checkRedisValueKind(container, RedisValueKind.HASH);

            Enumeration<String> keys = container.hash().keys();
            while (keys.hasMoreElements()) {
                ByteBuf buf = Unpooled.wrappedBuffer(keys.nextElement().getBytes(StandardCharsets.UTF_8));
                fields.add(new FullBulkStringRedisMessage(buf));
            }
        } finally {
            lock.readLock().unlock();
        }

        response.writeArray(fields);
    }
}
