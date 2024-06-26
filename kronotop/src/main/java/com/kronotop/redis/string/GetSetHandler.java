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
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.redis.string.protocol.GetSetMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

@Command(GetSetMessage.COMMAND)
@MaximumParameterCount(GetSetMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(GetSetMessage.MINIMUM_PARAMETER_COUNT)
public class GetSetHandler extends BaseStringHandler implements Handler {
    public GetSetHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.GETSET).set(new GetSetMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.GETSET).get().getKey());
    }

    @Override
    public void execute(Request request, Response response) {
        GetSetMessage getSetMessage = request.attr(MessageTypes.GETSET).get();

        Shard shard = service.findShard(getSetMessage.getKey());
        AtomicReference<Object> result = new AtomicReference<>();

        ReadWriteLock lock = shard.getStriped().get(getSetMessage.getKey());
        try {
            lock.writeLock().lock();
            shard.compute(getSetMessage.getKey(), (key, oldValue) -> {
                if (oldValue != null && !(oldValue instanceof StringValue)) {
                    throw new WrongTypeException();
                }
                if (oldValue == null) {
                    shard.getIndex().add(getSetMessage.getKey());
                }
                result.set(oldValue);
                return new StringValue(getSetMessage.getValue());
            });
        } finally {
            lock.writeLock().unlock();
        }

        if (result.get() == null) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }

        StringValue stringValue = (StringValue) result.get();
        ByteBuf buf = response.getChannelContext().alloc().buffer();
        buf.writeBytes(stringValue.getValue());
        shard.getPersistenceQueue().add(new StringKey(getSetMessage.getKey()));
        response.write(buf);
    }
}
