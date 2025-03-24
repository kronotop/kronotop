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

import com.kronotop.KronotopException;
import com.kronotop.RESPError;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.string.protocol.DecrByMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

@Command(DecrByMessage.COMMAND)
@MaximumParameterCount(DecrByMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(DecrByMessage.MINIMUM_PARAMETER_COUNT)
public class DecrByHandler extends BaseStringHandler implements Handler {
    public DecrByHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.DECRBY).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.DECRBY).set(new DecrByMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        DecrByMessage message = request.attr(MessageTypes.DECRBY).get();

        RedisShard shard = service.findShard(message.getKey(), ShardStatus.READWRITE);
        AtomicInteger result = new AtomicInteger();

        NumberManipulationHandler<Integer> handler = new NumberManipulationHandler<>(
                NumberManipulationHandler::encodeInteger,
                NumberManipulationHandler::decodeInteger
        );

        ReadWriteLock lock = shard.striped().get(message.getKey());
        lock.writeLock().lock();
        try {
            RedisValueContainer previous = handler.manipulate(shard, message.getKey(), (currentValue) -> {
                if (currentValue == null) {
                    currentValue = 0;
                }
                currentValue -= message.getDecrement();
                result.set(currentValue);
                return currentValue;
            });
            syncStringOnVolume(shard, message.getKey(), previous);
        } catch (NumberFormatException e) {
            throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER);
        } finally {
            lock.writeLock().unlock();
        }

        response.writeInteger(result.get());
    }
}
