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

import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.persistence.RedisValueContainer;
import com.kronotop.redis.storage.persistence.RedisValueKind;
import com.kronotop.redis.storage.persistence.jobs.AppendStringJob;
import com.kronotop.redis.string.protocol.AppendMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
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

    @Override
    public void execute(Request request, Response response) {
        AppendMessage appendMessage = request.attr(MessageTypes.APPEND).get();

        RedisShard shard = service.findShard(appendMessage.getKey());
        AtomicReference<Integer> result = new AtomicReference<>();

        ReadWriteLock lock = shard.striped().get(appendMessage.getKey());
        try {
            lock.writeLock().lock();
            shard.storage().compute(appendMessage.getKey(), (key, container) -> {
                if (container != null) {
                    if (!container.kind().equals(RedisValueKind.STRING)) {
                        throw new WrongTypeException();
                    }
                    StringValue value = container.string();
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    output.writeBytes(value.value());
                    output.writeBytes(appendMessage.getValue());
                    result.set(output.size());
                    return new RedisValueContainer(new StringValue(output.toByteArray()));
                } else {
                    shard.index().add(appendMessage.getKey());
                    result.set(appendMessage.getValue().length);
                    return new RedisValueContainer(new StringValue(appendMessage.getValue()));
                }
            });
        } finally {
            lock.writeLock().unlock();
        }

        shard.persistenceQueue().add(new AppendStringJob(appendMessage.getKey()));
        response.writeInteger(result.get());
    }
}
