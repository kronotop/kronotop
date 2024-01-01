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

import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.redis.HashValue;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.hash.protocol.FieldValuePair;
import com.kronotop.redis.hash.protocol.HIncrByMessage;
import com.kronotop.redis.storage.Shard;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(HIncrByMessage.COMMAND)
@MinimumParameterCount(HIncrByMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HIncrByMessage.MAXIMUM_PARAMETER_COUNT)
public class HIncrByHandler extends BaseHashHandler implements Handler {
    public HIncrByHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HINCRBY).set(new HIncrByMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.HINCRBY).get().getKey());
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HIncrByMessage hincrbyMessage = request.attr(MessageTypes.HINCRBY).get();

        Shard shard = service.findShard(hincrbyMessage.getKey());
        ReadWriteLock lock = shard.getStriped().get(hincrbyMessage.getKey());
        lock.writeLock().lock();
        long newValue;
        try {
            HashValue hashValue;
            Object retrieved = shard.get(hincrbyMessage.getKey());
            if (retrieved == null) {
                hashValue = new HashValue();
                shard.put(hincrbyMessage.getKey(), hashValue);
            } else {
                if (!(retrieved instanceof HashValue)) {
                    throw new WrongTypeException();
                }
                hashValue = (HashValue) retrieved;
            }

            FieldValuePair fieldValuePair = hincrbyMessage.getFieldValuePairs().get(0);
            if (fieldValuePair == null) {
                throw new KronotopException("field is missing");
            }

            byte[] oldValue = hashValue.get(fieldValuePair.getField());
            if (oldValue == null) {
                newValue = hincrbyMessage.getIncrement();
            } else {
                try {
                    newValue = Long.parseLong(new String(oldValue)) + hincrbyMessage.getIncrement();
                } catch (NumberFormatException e) {
                    throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_FLOAT, e);
                }
            }
            hashValue.put(fieldValuePair.getField(), Long.toString(newValue).getBytes());
        } finally {
            lock.writeLock().unlock();
        }

        persistence(shard, hincrbyMessage.getKey(), hincrbyMessage.getFieldValuePairs());
        response.writeInteger(newValue);
    }
}
