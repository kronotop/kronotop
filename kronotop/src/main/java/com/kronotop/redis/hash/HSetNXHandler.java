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

package com.kronotop.redis.hash;

import com.kronotop.redis.HashValue;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.hash.protocol.FieldValuePair;
import com.kronotop.redis.hash.protocol.HSetNXMessage;
import com.kronotop.redis.storage.Shard;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(HSetNXMessage.COMMAND)
@MaximumParameterCount(HSetNXMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(HSetNXMessage.MINIMUM_PARAMETER_COUNT)
public class HSetNXHandler extends BaseHashHandler implements Handler {
    public HSetNXHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HSETNX).set(new HSetNXMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.HSETNX).get().getKey());
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HSetNXMessage hsetnxMessage = request.attr(MessageTypes.HSETNX).get();

        Shard shard = service.findShard(hsetnxMessage.getKey());
        ReadWriteLock lock = shard.getStriped().get(hsetnxMessage.getKey());
        lock.writeLock().lock();
        int result = 0;
        try {
            HashValue hashValue;
            Object retrieved = shard.get(hsetnxMessage.getKey());
            if (retrieved == null) {
                hashValue = new HashValue();
                shard.put(hsetnxMessage.getKey(), hashValue);
            } else {
                if (!(retrieved instanceof HashValue)) {
                    throw new WrongTypeException();
                }
                hashValue = (HashValue) retrieved;
            }
            FieldValuePair fieldValuePair = hsetnxMessage.getFieldValuePairs().get(0);
            boolean exists = hashValue.containsKey(fieldValuePair.getField());
            if (!exists) {
                hashValue.put(fieldValuePair.getField(), fieldValuePair.getValue());
                result = 1;
            }
        } finally {
            lock.writeLock().unlock();
        }

        persistence(shard, hsetnxMessage.getKey(), hsetnxMessage.getFieldValuePairs());
        response.writeInteger(result);
    }
}
