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
import com.kronotop.redis.ResolveResponse;
import com.kronotop.redis.hash.protocol.FieldValuePair;
import com.kronotop.redis.hash.protocol.HIncrByFloatMessage;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

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

        ResolveResponse resolveResponse = service.resolveKey(hincrbyfloatMessage.getKey());
        if (resolveResponse.hasError()) {
            response.writeError(resolveResponse.getError());
            return;
        }

        LogicalDatabase storage = getLogicalDatabase(response.getContext());
        ReadWriteLock lock = storage.getStriped().get(hincrbyfloatMessage.getKey());
        lock.writeLock().lock();
        double newValue;
        try {
            HashValue hashValue;
            Object retrieved = storage.get(hincrbyfloatMessage.getKey());
            if (retrieved == null) {
                hashValue = new HashValue();
                storage.put(hincrbyfloatMessage.getKey(), hashValue);
            } else {
                if (!(retrieved instanceof HashValue)) {
                    throw new WrongTypeException(RESPError.WRONGTYPE_MESSAGE);
                }
                hashValue = (HashValue) retrieved;
            }

            FieldValuePair fieldValuePair = hincrbyfloatMessage.getFieldValuePairs().get(0);
            if (fieldValuePair == null) {
                throw new KronotopException("field is missing");
            }

            byte[] oldValue = hashValue.get(fieldValuePair.getField());
            if (oldValue == null) {
                newValue = hincrbyfloatMessage.getIncrement();
            } else {
                try {
                    newValue = Double.parseDouble(new String(oldValue)) + hincrbyfloatMessage.getIncrement();
                } catch (NumberFormatException e) {
                    throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER, e);
                }
            }
            hashValue.put(fieldValuePair.getField(), Double.toString(newValue).getBytes());
        } finally {
            lock.writeLock().unlock();
        }

        persistence(storage, hincrbyfloatMessage.getKey(), hincrbyfloatMessage.getFieldValuePairs());
        ByteBuf buf = response.getContext().alloc().buffer();
        buf.writeBytes(Double.toString(newValue).getBytes());
        response.write(buf);
    }
}

