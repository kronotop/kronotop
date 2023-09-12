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
import com.kronotop.redis.hash.protocol.HGetAllMessage;
import com.kronotop.redis.storage.Partition;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

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
        Partition partition = service.resolveKey(response.getContext(), hgetallMessage.getKey());
        ReadWriteLock lock = partition.getStriped().get(hgetallMessage.getKey());
        lock.readLock().lock();
        try {
            Object retrieved = partition.get(hgetallMessage.getKey());
            if (retrieved == null) {
                response.writeArray(result);
                return;
            }
            if (!(retrieved instanceof HashValue)) {
                throw new WrongTypeException();
            }

            HashValue hashValue = (HashValue) retrieved;
            Enumeration<String> fields = hashValue.keys();
            while (fields.hasMoreElements()) {
                String field = fields.nextElement();
                ByteBuf fieldBuf = response.getContext().alloc().buffer();
                fieldBuf.writeBytes(field.getBytes());
                result.add(new FullBulkStringRedisMessage(fieldBuf));

                byte[] value = hashValue.get(field);
                ByteBuf valueBuf = response.getContext().alloc().buffer();
                valueBuf.writeBytes(value);
                result.add(new FullBulkStringRedisMessage(valueBuf));
            }
        } finally {
            lock.readLock().unlock();
        }
        response.writeArray(result);
    }
}