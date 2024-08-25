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
import com.kronotop.redis.HashValue;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.hash.protocol.HValsMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(HValsMessage.COMMAND)
@MinimumParameterCount(HValsMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HValsMessage.MAXIMUM_PARAMETER_COUNT)
public class HValsHandler extends BaseHandler implements Handler {
    public HValsHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HVALS).set(new HValsMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HValsMessage hvalsMessage = request.attr(MessageTypes.HVALS).get();

        List<RedisMessage> result = new ArrayList<>();
        RedisShard shard = service.findShard(hvalsMessage.getKey());
        ReadWriteLock lock = shard.striped().get(hvalsMessage.getKey());
        lock.readLock().lock();
        try {
            Object retrieved = shard.storage().get(hvalsMessage.getKey());
            if (retrieved == null) {
                response.writeArray(result);
                return;
            }
            if (!(retrieved instanceof HashValue hashValue)) {
                throw new WrongTypeException();
            }

            Collection<byte[]> values = hashValue.values();
            for (byte[] value : values) {
                ByteBuf buf = response.getChannelContext().alloc().buffer();
                buf.writeBytes(value);
                result.add(new FullBulkStringRedisMessage(buf));
            }
        } finally {
            lock.readLock().unlock();
        }

        response.writeArray(result);
    }
}
