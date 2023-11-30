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
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.redis.string.protocol.DecrByMessage;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
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
        DecrByMessage decrByMessage = request.attr(MessageTypes.DECRBY).get();

        Shard shard = service.resolveKey(response.getContext(), decrByMessage.getKey());
        AtomicReference<Integer> result = new AtomicReference<>();

        ReadWriteLock lock = shard.getStriped().get(decrByMessage.getKey());
        try {
            lock.writeLock().lock();
            shard.compute(decrByMessage.getKey(), (key, oldValue) -> {
                int currentValue = 0;
                if (oldValue != null) {
                    StringValue value = (StringValue) oldValue;
                    try {
                        currentValue = Integer.parseInt(new String(value.getValue()));
                    } catch (NumberFormatException e) {
                        throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER, e);
                    }
                } else {
                    // New key
                    shard.getIndex().add(decrByMessage.getKey());
                }
                currentValue -= decrByMessage.getDecrement();
                result.set(currentValue);
                return new StringValue(Integer.toString(currentValue).getBytes());
            });
        } finally {
            lock.writeLock().unlock();
        }

        shard.getPersistenceQueue().add(new StringKey(decrByMessage.getKey()));
        response.writeInteger(result.get());
    }
}
