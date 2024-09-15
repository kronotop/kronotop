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

package com.kronotop.redis.string;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.persistence.RedisValueContainer;
import com.kronotop.redis.storage.persistence.RedisValueKind;
import com.kronotop.redis.string.protocol.GetMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.locks.ReadWriteLock;

@Command(GetMessage.COMMAND)
@MaximumParameterCount(GetMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(GetMessage.MINIMUM_PARAMETER_COUNT)
public class GetHandler extends BaseStringHandler implements Handler {
    public GetHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.GET).set(new GetMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        GetMessage getMessage = request.attr(MessageTypes.GET).get();

        RedisShard shard = service.findShard(getMessage.getKey());
        ReadWriteLock lock = shard.striped().get(getMessage.getKey());
        try {
            lock.readLock().lock();
            RedisValueContainer container = shard.storage().get(getMessage.getKey());
            if (container == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }
            if (!container.kind().equals(RedisValueKind.STRING)) {
                throw new WrongTypeException();
            }
            ByteBuf buf = response.getChannelContext().alloc().buffer();
            buf.writeBytes(container.string().value());
            response.write(buf);
        } finally {
            lock.readLock().unlock();
        }
    }
}
