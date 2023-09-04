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
import com.kronotop.redis.storage.Partition;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.redis.string.protocol.IncrByMessage;
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

@Command(IncrByMessage.COMMAND)
@MaximumParameterCount(IncrByMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(IncrByMessage.MINIMUM_PARAMETER_COUNT)
public class IncrByHandler extends BaseStringHandler implements Handler {
    public IncrByHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.INCRBY).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.INCRBY).set(new IncrByMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        IncrByMessage incrByMessage = request.attr(MessageTypes.INCRBY).get();

        Partition partition = service.resolveKey(response.getContext(), incrByMessage.getKey());
        AtomicReference<Integer> result = new AtomicReference<>();
        ReadWriteLock lock = partition.getStriped().get(incrByMessage.getKey());
        try {
            lock.writeLock().lock();
            partition.compute(incrByMessage.getKey(), (key, oldValue) -> {
                int currentValue = 0;
                if (oldValue != null) {
                    StringValue value = (StringValue) oldValue;
                    try {
                        currentValue = Integer.parseInt(new String(value.getValue()));
                    } catch (NumberFormatException e) {
                        throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER, e);
                    }
                } else {
                    partition.getIndex().update(incrByMessage.getKey());
                }
                currentValue += incrByMessage.getIncrement();
                result.set(currentValue);
                return new StringValue(Integer.toString(currentValue).getBytes());
            });
        } finally {
            lock.writeLock().unlock();
        }

        partition.getPersistenceQueue().add(new StringKey(incrByMessage.getKey()));
        response.writeInteger(result.get());
    }
}
