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

import com.kronotop.redis.RedisService;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.redis.string.protocol.SetNXMessage;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

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

        Shard shard = service.resolveKey(setnxMessage.getKey());
        Object result;
        ReadWriteLock lock = shard.getStriped().get(setnxMessage.getKey());
        try {
            lock.writeLock().lock();
            result = shard.putIfAbsent(
                    setnxMessage.getKey(),
                    new StringValue(setnxMessage.getValue())
            );
            if (result == null) {
                shard.getIndex().add(setnxMessage.getKey());
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (result == null) {
            response.writeInteger(1);
        } else {
            response.writeInteger(0);
        }

        shard.getPersistenceQueue().add(new StringKey(setnxMessage.getKey()));
    }
}
