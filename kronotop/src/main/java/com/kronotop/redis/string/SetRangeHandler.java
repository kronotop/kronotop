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
import com.kronotop.redis.ResolveResponse;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.redis.string.protocol.SetRangeMessage;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

@Command(SetRangeMessage.COMMAND)
@MaximumParameterCount(SetRangeMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SetRangeMessage.MINIMUM_PARAMETER_COUNT)
public class SetRangeHandler extends BaseStringHandler implements Handler {
    public SetRangeHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.SETRANGE).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SETRANGE).set(new SetRangeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        SetRangeMessage setRangeMessage = request.attr(MessageTypes.SETRANGE).get();

        ResolveResponse resolveResponse = service.resolveKey(setRangeMessage.getKey());
        if (resolveResponse.hasError()) {
            response.writeError(resolveResponse.getError());
            return;
        }

        LogicalDatabase storage = getLogicalDatabase(response.getContext());
        AtomicReference<Integer> result = new AtomicReference<>();

        ReadWriteLock lock = storage.getStriped().get(setRangeMessage.getKey());
        try {
            lock.writeLock().lock();
            storage.compute(setRangeMessage.getKey(), (key, oldValue) -> {
                if (oldValue == null) {
                    int offset = setRangeMessage.getOffset();
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    byte[] padding = new byte[offset];
                    output.writeBytes(padding);
                    output.writeBytes(setRangeMessage.getValue());
                    result.set(output.size());
                    storage.getIndex().update(setRangeMessage.getKey());
                    return new StringValue(output.toByteArray());
                }

                StringValue value = (StringValue) oldValue;
                int size = value.getValue().length;
                int overflowSize = value.getValue().length - (setRangeMessage.getOffset() + setRangeMessage.getValue().length);
                if (overflowSize < 0) {
                    size += Math.abs(overflowSize);
                }
                byte[] data = new byte[size];
                ByteBuffer buf = ByteBuffer.wrap(data);
                buf.put(value.getValue());
                buf.position(setRangeMessage.getOffset());
                buf.put(setRangeMessage.getValue());

                result.set(size);
                return new StringValue(buf.array());
            });
        } finally {
            lock.writeLock().unlock();
        }
        storage.getPersistenceQueue().add(new StringKey(setRangeMessage.getKey()));
        response.writeInteger(result.get());
    }

}
