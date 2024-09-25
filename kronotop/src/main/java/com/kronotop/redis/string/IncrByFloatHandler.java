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

import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.string.protocol.IncrByFloatMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

@Command(IncrByFloatMessage.COMMAND)
@MaximumParameterCount(IncrByFloatMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(IncrByFloatMessage.MINIMUM_PARAMETER_COUNT)
public class IncrByFloatHandler extends BaseStringHandler implements Handler {
    public IncrByFloatHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.INCRBYFLOAT).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.INCRBYFLOAT).set(new IncrByFloatMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        IncrByFloatMessage message = request.attr(MessageTypes.INCRBYFLOAT).get();

        RedisShard shard = service.findShard(message.getKey());
        AtomicReference<Double> result = new AtomicReference<>();

        NumberManipulationHandler<Double> handler = new NumberManipulationHandler<>(
                NumberManipulationHandler::encodeDouble,
                NumberManipulationHandler::decodeDouble
        );
        ReadWriteLock lock = shard.striped().get(message.getKey());
        lock.writeLock().lock();
        try {
            RedisValueContainer previous = handler.manipulate(shard, message.getKey(), (currentValue) -> {
                if (currentValue == null) {
                    currentValue = 0.0;
                }
                currentValue += message.getIncrement();
                result.set(currentValue);
                return currentValue;
            });
            syncStringOnVolume(shard, message.getKey(), previous);
        } catch (NumberFormatException e) {
            throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_FLOAT, e);
        } finally {
            lock.writeLock().unlock();
        }

        ByteBuf buf = response.getChannelContext().alloc().buffer();
        buf.writeBytes(result.get().toString().getBytes());
        response.write(buf);
    }
}
