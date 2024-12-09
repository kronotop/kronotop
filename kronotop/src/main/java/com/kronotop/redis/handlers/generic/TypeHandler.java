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

package com.kronotop.redis.handlers.generic;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.handlers.generic.protocol.TypeMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.concurrent.locks.ReadWriteLock;

@Command(TypeMessage.COMMAND)
@MinimumParameterCount(TypeMessage.MINIMUM_PARAMETER_COUNT)
public class TypeHandler extends BaseHandler implements Handler {
    public TypeHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.TYPE).set(new TypeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        TypeMessage typeMessage = request.attr(MessageTypes.TYPE).get();

        RedisShard shard = service.findShard(typeMessage.getKey(), ShardStatus.READONLY);

        RedisValueContainer container;
        ReadWriteLock lock = shard.striped().get(typeMessage.getKey());
        lock.readLock().lock();
        try {
            container = shard.storage().get(typeMessage.getKey());
        } finally {
            lock.readLock().unlock();
        }

        if (container == null) {
            response.writeSimpleString("none");
            return;
        }

        String dataStructureType = container.kind().toString().toLowerCase();
        response.writeSimpleString(dataStructureType);
    }
}
