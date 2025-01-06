/*
 * Copyright (c) 2023-2025 Kronotop
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
import com.kronotop.redis.handlers.string.protocol.SetNXMessage;
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
        SetNXMessage message = request.attr(MessageTypes.SETNX).get();

        RedisShard shard = service.findShard(message.getKey(), ShardStatus.READWRITE);
        RedisValueContainer previous;
        ReadWriteLock lock = shard.striped().get(message.getKey());
        lock.writeLock().lock();
        try {
            previous = shard.storage().putIfAbsent(
                    message.getKey(),
                    new RedisValueContainer(new StringValue(message.getValue()))
            );
            syncStringOnVolume(shard, message.getKey(), previous);
        } finally {
            lock.writeLock().unlock();
        }

        if (previous == null) {
            response.writeInteger(1);
            return;
        }
        response.writeInteger(0);
    }
}
