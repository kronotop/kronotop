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
import com.kronotop.redis.hash.protocol.HMGetMessage;
import com.kronotop.redis.storage.Shard;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(HMGetMessage.COMMAND)
@MinimumParameterCount(HMGetMessage.MINIMUM_PARAMETER_COUNT)
public class HMGetHandler extends BaseHandler implements Handler {
    public HMGetHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HMGET).set(new HMGetMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HMGetMessage hmgetMessage = request.attr(MessageTypes.HMGET).get();

        List<RedisMessage> upperList = new ArrayList<>();
        Shard shard = service.findShard(hmgetMessage.getKey());
        ReadWriteLock lock = shard.getStriped().get(hmgetMessage.getKey());
        lock.readLock().lock();
        try {
            Object retrieved = shard.get(hmgetMessage.getKey());
            if (retrieved == null) {
                for (int i = 0; i < hmgetMessage.getFields().size(); i++) {
                    upperList.add(FullBulkStringRedisMessage.NULL_INSTANCE);
                }
            } else {
                if (!(retrieved instanceof HashValue)) {
                    throw new WrongTypeException();
                }

                HashValue hashValue = (HashValue) retrieved;
                for (String field : hmgetMessage.getFields()) {
                    byte[] value = hashValue.get(field);
                    if (value == null) {
                        upperList.add(FullBulkStringRedisMessage.NULL_INSTANCE);
                        continue;
                    }

                    ByteBuf buf = response.getChannelContext().alloc().buffer();
                    buf.writeBytes(value);
                    upperList.add(new FullBulkStringRedisMessage(buf));
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        response.writeArray(upperList);
    }
}
