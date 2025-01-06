/*
 * Copyright (c) 2023-2025 Kronotop
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
import com.kronotop.redis.handlers.string.protocol.SetRangeMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

@Command(SetRangeMessage.COMMAND)
@MaximumParameterCount(SetRangeMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SetRangeMessage.MINIMUM_PARAMETER_COUNT)
public class SetRangeHandler extends BaseStringHandler implements Handler {
    public SetRangeHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.SETRANGE).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SETRANGE).set(new SetRangeMessage(request));
    }

    private RedisValueContainer executeSetRangeCommand(RedisShard shard, SetRangeMessage message, AtomicInteger result) {
        RedisValueContainer previous = shard.storage().get(message.getKey());
        if (previous == null) {
            int offset = message.getOffset();
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] padding = new byte[offset];
            output.writeBytes(padding);
            output.writeBytes(message.getValue());
            result.set(output.size());
            shard.index().add(message.getKey());
            RedisValueContainer container = new RedisValueContainer(new StringValue(output.toByteArray()));
            shard.storage().put(message.getKey(), container);
            return null;
        }

        int size = previous.string().value().length;
        int overflowSize = previous.string().value().length - (message.getOffset() + message.getValue().length);
        if (overflowSize < 0) {
            size += Math.abs(overflowSize);
        }
        byte[] data = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.put(previous.string().value());
        buf.position(message.getOffset());
        buf.put(message.getValue());

        result.set(size);
        RedisValueContainer container = new RedisValueContainer(new StringValue(buf.array()));
        shard.storage().put(message.getKey(), container);
        return previous;
    }

    @Override
    public void execute(Request request, Response response) {
        SetRangeMessage message = request.attr(MessageTypes.SETRANGE).get();

        RedisShard shard = service.findShard(message.getKey(), ShardStatus.READWRITE);
        AtomicInteger result = new AtomicInteger();

        ReadWriteLock lock = shard.striped().get(message.getKey());
        lock.writeLock().lock();
        try {
            RedisValueContainer previous = executeSetRangeCommand(shard, message, result);
            syncStringOnVolume(shard, message.getKey(), previous);
        } finally {
            lock.writeLock().unlock();
        }
        response.writeInteger(result.get());
    }
}
