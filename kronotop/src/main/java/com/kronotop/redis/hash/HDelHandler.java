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

package com.kronotop.redis.hash;

import com.kronotop.redis.HashValue;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.hash.protocol.FieldValuePair;
import com.kronotop.redis.hash.protocol.HDelMessage;
import com.kronotop.redis.storage.Shard;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(HDelMessage.COMMAND)
@MinimumParameterCount(HDelMessage.MINIMUM_PARAMETER_COUNT)
public class HDelHandler extends BaseHashHandler implements Handler {
    public HDelHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HDEL).set(new HDelMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.HDEL).get().getKey());
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HDelMessage hdelMessage = request.attr(MessageTypes.HDEL).get();

        int total = 0;

        Shard shard = service.findShard(hdelMessage.getKey());
        ReadWriteLock lock = shard.getStriped().get(hdelMessage.getKey());
        lock.writeLock().lock();
        try {
            Object retrieved = shard.get(hdelMessage.getKey());
            if (retrieved == null) {
                response.writeInteger(0);
                return;
            }
            if (!(retrieved instanceof HashValue)) {
                throw new WrongTypeException();
            }

            HashValue hashValue = (HashValue) retrieved;
            for (FieldValuePair fieldValuePair : hdelMessage.getFieldValuePairs()) {
                byte[] value = hashValue.remove(fieldValuePair.getField());
                if (value != null) {
                    total++;
                }
            }
            if (hashValue.size() == 0) {
                shard.remove(hdelMessage.getKey());
            }
        } finally {
            lock.writeLock().unlock();
        }
        persistence(shard, hdelMessage.getKey(), hdelMessage.getFieldValuePairs());
        response.writeInteger(total);
    }
}
