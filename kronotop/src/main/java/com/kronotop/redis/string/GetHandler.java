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

package com.kronotop.redis.string;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.Shard;
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

        Shard shard = service.findShard(getMessage.getKey());
        ReadWriteLock lock = shard.getStriped().get(getMessage.getKey());
        Object retrieved;
        try {
            lock.readLock().lock();
            retrieved = shard.get(getMessage.getKey());
        } finally {
            lock.readLock().unlock();
        }

        if (retrieved == null) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        if (!(retrieved instanceof StringValue)) {
            throw new WrongTypeException();
        }
        StringValue stringValue = (StringValue) retrieved;
        ByteBuf buf = response.getContext().alloc().buffer();
        buf.writeBytes(stringValue.getValue());
        response.write(buf);
    }
}
