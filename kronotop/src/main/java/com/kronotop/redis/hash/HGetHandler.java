/*
 * Copyright (c) 2023 Kronotop
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
import com.kronotop.redis.HashValue;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.hash.protocol.HGetMessage;
import com.kronotop.redis.storage.Shard;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;

import java.util.concurrent.locks.ReadWriteLock;

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

        Shard shard = service.resolveKey(hgetMessage.getKey());
        ReadWriteLock lock = shard.getStriped().get(hgetMessage.getKey());
        lock.readLock().lock();
        try {
            Object retrieved = shard.get(hgetMessage.getKey());
            if (retrieved == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }
            if (!(retrieved instanceof HashValue)) {
                throw new WrongTypeException();
            }

            HashValue hashValue = (HashValue) retrieved;
            byte[] value = hashValue.get(hgetMessage.getField());
            if (value == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }

            ByteBuf buf = response.getContext().alloc().buffer();
            buf.writeBytes(value);
            response.write(buf);
        } finally {
            lock.readLock().unlock();
        }
    }
}
