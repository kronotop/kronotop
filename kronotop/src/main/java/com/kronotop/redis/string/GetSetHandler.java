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
import com.kronotop.redis.storage.persistence.jobs.AppendStringJob;
import com.kronotop.redis.string.protocol.GetSetMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.redis.RedisService.checkRedisValueKind;

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

        RedisShard shard = service.findShard(getSetMessage.getKey());
        AtomicReference<StringValue> result = new AtomicReference<>();

        ReadWriteLock lock = shard.striped().get(getSetMessage.getKey());
        try {
            lock.writeLock().lock();
            shard.storage().compute(getSetMessage.getKey(), (key, container) -> {
                if (container == null) {
                    shard.index().add(getSetMessage.getKey());
                } else {
                    checkRedisValueKind(container, RedisValueKind.STRING);
                    result.set(container.string());
                }
                return new RedisValueContainer(new StringValue(getSetMessage.getValue()));
            });
        } finally {
            lock.writeLock().unlock();
        }

        if (result.get() == null) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }

        ByteBuf buf = response.getChannelContext().alloc().buffer();
        buf.writeBytes(result.get().value());
        shard.persistenceQueue().add(new AppendStringJob(getSetMessage.getKey()));
        response.write(buf);
    }
}
