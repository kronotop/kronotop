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

package com.kronotop.redis.handlers.string;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.string.protocol.GetSetMessage;
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
import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
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

    private RedisValueContainer executeGetSetCommand(RedisShard shard, GetSetMessage message) {
        RedisValueContainer previous = shard.storage().get(message.getKey());
        checkRedisValueKind(previous, RedisValueKind.STRING);

        RedisValueContainer container = new RedisValueContainer(new StringValue(message.getValue()));
        shard.storage().put(message.getKey(), container);

        return previous;
    }

    @Override
    public void execute(Request request, Response response) {
        GetSetMessage message = request.attr(MessageTypes.GETSET).get();

        RedisValueContainer previous;
        RedisShard shard = service.findShard(message.getKey(), ShardStatus.READWRITE);
        ReadWriteLock lock = shard.striped().get(message.getKey());
        lock.writeLock().lock();
        try {
            previous = executeGetSetCommand(shard, message);
            syncStringOnVolume(shard, message.getKey(), previous);
        } finally {
            lock.writeLock().unlock();
        }

        if (previous == null) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }

        ByteBuf buf = response.getCtx().alloc().buffer();
        buf.writeBytes(previous.string().value());
        response.write(buf);
    }
}
