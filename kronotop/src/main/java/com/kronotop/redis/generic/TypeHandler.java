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

package com.kronotop.redis.generic;

import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.generic.protocol.TypeMessage;
import com.kronotop.redis.storage.Shard;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

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

        Shard shard = service.findShard(typeMessage.getKey());

        Object retrieved;
        ReadWriteLock lock = shard.getStriped().get(typeMessage.getKey());
        try {
            lock.readLock().lock();
            retrieved = shard.get(typeMessage.getKey());
        } finally {
            lock.readLock().unlock();
        }

        if (retrieved == null) {
            response.writeSimpleString("none");
            return;
        }

        if (retrieved instanceof StringValue) {
            response.writeSimpleString("string");
        } else {
            response.writeError("unknown type");
        }
    }
}
