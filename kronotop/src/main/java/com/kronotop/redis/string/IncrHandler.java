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
import com.kronotop.redis.storage.persistence.RedisValueContainer;
import com.kronotop.redis.storage.persistence.jobs.AppendStringJob;
import com.kronotop.redis.string.protocol.IncrMessage;
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

@Command(IncrMessage.COMMAND)
@MaximumParameterCount(IncrMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(IncrMessage.MINIMUM_PARAMETER_COUNT)
public class IncrHandler extends BaseStringHandler implements Handler {
    public IncrHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.INCR).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.INCR).set(new IncrMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        IncrMessage incrMessage = request.attr(MessageTypes.INCR).get();

        RedisShard shard = service.findShard(incrMessage.getKey());

        ReadWriteLock lock = shard.striped().get(incrMessage.getKey());
        AtomicReference<Integer> result = new AtomicReference<>();
        try {
            lock.writeLock().lock();
            shard.storage().compute(incrMessage.getKey(), (key, container) -> {
                int currentValue = 0;
                if (container != null) {
                    try {
                        currentValue = Integer.parseInt(new String(container.string().value()));
                    } catch (NumberFormatException e) {
                        throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER, e);
                    }
                } else {
                    shard.index().add(incrMessage.getKey());
                }
                currentValue += 1;
                result.set(currentValue);
                StringValue value = new StringValue(Integer.toString(currentValue).getBytes());
                return new RedisValueContainer(value);
            });
        } finally {
            lock.writeLock().unlock();
        }

        shard.persistenceQueue().add(new AppendStringJob(incrMessage.getKey()));
        response.writeInteger(result.get());
    }
}
