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

import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.hash.protocol.FieldValuePair;
import com.kronotop.redis.hash.protocol.HIncrByFloatMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.persistence.RedisValueContainer;
import com.kronotop.redis.storage.persistence.RedisValueKind;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.redis.RedisService.checkRedisValueKind;

@Command(HIncrByFloatMessage.COMMAND)
@MinimumParameterCount(HIncrByFloatMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HIncrByFloatMessage.MAXIMUM_PARAMETER_COUNT)
public class HIncrByFloatHandler extends BaseHashHandler implements Handler {
    public HIncrByFloatHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HINCRBYFLOAT).set(new HIncrByFloatMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.HINCRBYFLOAT).get().getKey());
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HIncrByFloatMessage hincrbyfloatMessage = request.attr(MessageTypes.HINCRBYFLOAT).get();

        RedisShard shard = service.findShard(hincrbyfloatMessage.getKey());
        ReadWriteLock lock = shard.striped().get(hincrbyfloatMessage.getKey());
        lock.writeLock().lock();
        double newValue;
        try {
            HashValue hashValue;
            RedisValueContainer container = shard.storage().get(hincrbyfloatMessage.getKey());
            if (container == null) {
                hashValue = new HashValue();
                shard.storage().put(hincrbyfloatMessage.getKey(), new RedisValueContainer(hashValue));
            } else {
                checkRedisValueKind(container, RedisValueKind.HASH);
                hashValue = container.hash();
            }

            FieldValuePair fieldValuePair = hincrbyfloatMessage.getFieldValuePairs().getFirst();
            if (fieldValuePair == null) {
                throw new KronotopException("field is missing");
            }

            HashFieldValue oldHashField = hashValue.get(fieldValuePair.getField());
            if (oldHashField == null) {
                newValue = hincrbyfloatMessage.getIncrement();
            } else {
                try {
                    newValue = Double.parseDouble(new String(oldHashField.value())) + hincrbyfloatMessage.getIncrement();
                } catch (NumberFormatException e) {
                    throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER, e);
                }
            }
            hashValue.put(fieldValuePair.getField(), new HashFieldValue(Double.toString(newValue).getBytes()));
        } finally {
            lock.writeLock().unlock();
        }

        persistence(shard, hincrbyfloatMessage.getKey(), hincrbyfloatMessage.getFieldValuePairs());
        ByteBuf buf = response.getChannelContext().alloc().buffer();
        buf.writeBytes(Double.toString(newValue).getBytes());
        response.write(buf);
    }
}

