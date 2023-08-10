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

import com.kronotop.common.resp.RESPError;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.ResolveResponse;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.redis.string.protocol.GetDelMessage;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(GetDelMessage.COMMAND)
@MaximumParameterCount(GetDelMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(GetDelMessage.MINIMUM_PARAMETER_COUNT)
public class GetDelHandler extends BaseStringHandler implements Handler {
    public GetDelHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.GETDEL).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.GETDEL).set(new GetDelMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        GetDelMessage getDelMessage = request.attr(MessageTypes.GETDEL).get();

        ResolveResponse resolveResponse = service.resolveKey(getDelMessage.getKey());
        if (resolveResponse.hasError()) {
            response.writeError(resolveResponse.getError());
            return;
        }

        LogicalDatabase storage = getLogicalDatabase(response.getContext());
        ReadWriteLock lock = storage.getStriped().get(getDelMessage.getKey());

        Object retrieved;
        try {
            lock.writeLock().lock();
            retrieved = storage.remove(getDelMessage.getKey());
            if (retrieved != null) {
                storage.getIndex().drop(getDelMessage.getKey());
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (retrieved == null) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        if (!(retrieved instanceof StringValue)) {
            throw new WrongTypeException(RESPError.WRONGTYPE_MESSAGE);
        }
        StringValue stringValue = (StringValue) retrieved;
        ByteBuf buf = response.getContext().alloc().buffer();
        buf.writeBytes(stringValue.getValue());
        storage.getPersistenceQueue().add(new StringKey(getDelMessage.getKey()));
        response.write(buf);
    }
}
