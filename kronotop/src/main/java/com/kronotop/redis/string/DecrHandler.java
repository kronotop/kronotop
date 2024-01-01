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
import com.kronotop.redis.string.protocol.DecrMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

@Command(DecrMessage.COMMAND)
@MaximumParameterCount(DecrMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(DecrMessage.MINIMUM_PARAMETER_COUNT)
public class DecrHandler extends BaseStringHandler implements Handler {
    public DecrHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.DECR).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.DECR).set(new DecrMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        DecrMessage decrMessage = request.attr(MessageTypes.DECR).get();

        Shard shard = service.findShard(decrMessage.getKey());
        AtomicReference<Integer> result = new AtomicReference<>();

        ReadWriteLock lock = shard.getStriped().get(decrMessage.getKey());
        try {
            lock.writeLock().lock();
            shard.compute(decrMessage.getKey(), (key, oldValue) -> {
                int currentValue = 0;
                if (oldValue != null) {
                    StringValue value = (StringValue) oldValue;
                    try {
                        currentValue = Integer.parseInt(new String(value.getValue()));
                    } catch (NumberFormatException e) {
                        throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER, e);
                    }
                } else {
                    shard.getIndex().add(decrMessage.getKey());
                }
                currentValue -= 1;
                result.set(currentValue);
                return new StringValue(Integer.toString(currentValue).getBytes());
            });
        } finally {
            lock.writeLock().unlock();
        }

        shard.getPersistenceQueue().add(new StringKey(decrMessage.getKey()));
        response.writeInteger(result.get());
    }
}
