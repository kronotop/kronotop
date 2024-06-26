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
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.string.protocol.StrlenMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.concurrent.locks.ReadWriteLock;

@Command(StrlenMessage.COMMAND)
@MaximumParameterCount(StrlenMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(StrlenMessage.MINIMUM_PARAMETER_COUNT)
public class StrlenHandler extends BaseStringHandler implements Handler {
    public StrlenHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.STRLEN).set(new StrlenMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        StrlenMessage strlenMessage = request.attr(MessageTypes.STRLEN).get();

        Shard shard = service.findShard(strlenMessage.getKey());
        Object received;
        ReadWriteLock lock = shard.getStriped().get(strlenMessage.getKey());
        try {
            lock.readLock().lock();
            received = shard.get(strlenMessage.getKey());
        } finally {
            lock.readLock().unlock();
        }

        if (received == null) {
            response.writeInteger(0);
            return;
        }
        if (!(received instanceof StringValue)) {
            throw new WrongTypeException();
        }
        StringValue stringValue = (StringValue) received;
        response.writeInteger(stringValue.getValue().length);
    }
}
