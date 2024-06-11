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
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.string.protocol.MGetMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(MGetMessage.COMMAND)
@MinimumParameterCount(MGetMessage.MINIMUM_PARAMETER_COUNT)
public class MGetHandler extends BaseStringHandler implements Handler {
    public MGetHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.MGET).set(new MGetMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        MGetMessage mgetMessage = request.attr(MessageTypes.MGET).get();

        Shard shard = service.findShard(mgetMessage.getKeys());

        Iterable<ReadWriteLock> locks = shard.getStriped().bulkGet(mgetMessage.getKeys());
        List<RedisMessage> result = new ArrayList<>();
        try {
            for (ReadWriteLock lock : locks) {
                lock.readLock().lock();
            }

            for (String key : mgetMessage.getKeys()) {
                Object value = shard.get(key);
                if (value == null) {
                    result.add(FullBulkStringRedisMessage.NULL_INSTANCE);
                    continue;
                }

                if (!(value instanceof StringValue)) {
                    result.add(FullBulkStringRedisMessage.NULL_INSTANCE);
                    continue;
                }

                StringValue stringValue = (StringValue) value;
                ByteBuf buf = response.getChannelContext().alloc().buffer();
                buf.writeBytes(stringValue.getValue());
                result.add(new FullBulkStringRedisMessage(buf));
            }
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.readLock().unlock();
            }
        }

        response.writeArray(result);
    }
}
