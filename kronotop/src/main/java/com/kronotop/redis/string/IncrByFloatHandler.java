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

import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.ResolveResponse;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.redis.string.protocol.IncrByFloatMessage;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
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
        IncrByFloatMessage incrByFloatMessage = request.attr(MessageTypes.INCRBYFLOAT).get();

        ResolveResponse resolveResponse = service.resolveKey(incrByFloatMessage.getKey());
        if (resolveResponse.hasError()) {
            response.writeError(resolveResponse.getError());
            return;
        }

        LogicalDatabase storage = getLogicalDatabase(response.getContext());
        AtomicReference<Double> result = new AtomicReference<>();
        ReadWriteLock lock = storage.getStriped().get(incrByFloatMessage.getKey());

        try {
            lock.writeLock().lock();
            storage.compute(incrByFloatMessage.getKey(), (key, oldValue) -> {
                double currentValue = 0;
                if (oldValue != null) {
                    StringValue value = (StringValue) oldValue;
                    try {
                        currentValue = Double.parseDouble(new String(value.getValue()));
                    } catch (NumberFormatException e) {
                        throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_FLOAT, e);
                    }
                } else {
                    storage.getIndex().update(incrByFloatMessage.getKey());
                }
                currentValue += incrByFloatMessage.getIncrement();
                result.set(currentValue);
                return new StringValue(Double.toString(currentValue).getBytes());
            });
        } finally {
            lock.writeLock().unlock();
        }

        storage.getPersistenceQueue().add(new StringKey(incrByFloatMessage.getKey()));
        ByteBuf buf = response.getContext().alloc().buffer();
        buf.writeBytes(result.get().toString().getBytes());
        response.write(buf);
    }
}
