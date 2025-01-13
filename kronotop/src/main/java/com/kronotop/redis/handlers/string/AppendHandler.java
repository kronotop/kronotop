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
import com.kronotop.redis.handlers.string.protocol.AppendMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.RedisValueKind;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

@Command(AppendMessage.COMMAND)
@MaximumParameterCount(AppendMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(AppendMessage.MINIMUM_PARAMETER_COUNT)
public class AppendHandler extends BaseStringHandler implements Handler {
    public AppendHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.APPEND).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.APPEND).set(new AppendMessage(request));
    }

    private RedisValueContainer appendCommand(RedisShard shard, AppendMessage message, AtomicInteger result) {
        RedisValueContainer previous = shard.storage().get(message.getKey());
        if (previous == null) {
            StringValue newValue = new StringValue(message.getValue());
            shard.storage().put(message.getKey(), new RedisValueContainer(newValue));
            result.set(newValue.value().length);
            return null;
        }

        if (!previous.kind().equals(RedisValueKind.STRING)) {
            throw new WrongTypeException();
        }

        StringValue previousValue = previous.string();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.writeBytes(previousValue.value());
        output.writeBytes(message.getValue());

        StringValue newValue = new StringValue(output.toByteArray());
        shard.storage().put(message.getKey(), new RedisValueContainer(newValue));
        result.set(newValue.value().length);
        return previous;
    }

    @Override
    public void execute(Request request, Response response) {
        AppendMessage message = request.attr(MessageTypes.APPEND).get();

        RedisShard shard = service.findShard(message.getKey(), ShardStatus.READWRITE);
        AtomicInteger result = new AtomicInteger();

        ReadWriteLock lock = shard.striped().get(message.getKey());
        try {
            lock.writeLock().lock();
            RedisValueContainer previous = appendCommand(shard, message, result);
            syncStringOnVolume(shard, message.getKey(), previous);
        } finally {
            lock.writeLock().unlock();
        }
        response.writeInteger(result.get());
    }
}
