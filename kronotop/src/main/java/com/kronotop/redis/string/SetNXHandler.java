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
import com.kronotop.redis.storage.persistence.jobs.AppendStringJob;
import com.kronotop.redis.string.protocol.SetNXMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(SetNXMessage.COMMAND)
@MaximumParameterCount(SetNXMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SetNXMessage.MINIMUM_PARAMETER_COUNT)
public class SetNXHandler extends BaseStringHandler implements Handler {
    public SetNXHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SETNX).set(new SetNXMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.SETNX).get().getKey());
    }

    @Override
    public void execute(Request request, Response response) {
        SetNXMessage setnxMessage = request.attr(MessageTypes.SETNX).get();

        RedisShard shard = service.findShard(setnxMessage.getKey());
        RedisValueContainer container;
        ReadWriteLock lock = shard.striped().get(setnxMessage.getKey());
        try {
            lock.writeLock().lock();
            container = shard.storage().putIfAbsent(
                    setnxMessage.getKey(),
                    new RedisValueContainer(new StringValue(setnxMessage.getValue()))
            );
            if (container == null) {
                shard.index().add(setnxMessage.getKey());
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (container == null) {
            response.writeInteger(1);
        } else {
            response.writeInteger(0);
        }

        shard.persistenceQueue().add(new AppendStringJob(setnxMessage.getKey()));
    }
}
