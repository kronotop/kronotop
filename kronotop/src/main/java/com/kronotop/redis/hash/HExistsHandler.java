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

import com.kronotop.common.resp.RESPError;
import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.HashValue;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.ResolveResponse;
import com.kronotop.redis.hash.protocol.HExistsMessage;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

import java.util.concurrent.locks.ReadWriteLock;

@Command(HExistsMessage.COMMAND)
@MinimumParameterCount(HExistsMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HExistsMessage.MAXIMUM_PARAMETER_COUNT)
public class HExistsHandler extends BaseHandler implements Handler {
    public HExistsHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HEXISTS).set(new HExistsMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HExistsMessage hexistsMessage = request.attr(MessageTypes.HEXISTS).get();

        ResolveResponse resolveResponse = service.resolveKey(hexistsMessage.getKey());
        if (resolveResponse.hasError()) {
            response.writeError(resolveResponse.getError());
            return;
        }

        LogicalDatabase storage = getLogicalDatabase(response.getContext());
        ReadWriteLock lock = storage.getStriped().get(hexistsMessage.getKey());
        lock.readLock().lock();
        try {
            Object retrieved = storage.get(hexistsMessage.getKey());
            if (retrieved == null) {
                response.writeInteger(0);
                return;
            }
            if (!(retrieved instanceof HashValue)) {
                throw new WrongTypeException(RESPError.WRONGTYPE_MESSAGE);
            }

            HashValue hashValue = (HashValue) retrieved;
            boolean exists = hashValue.containsKey(hexistsMessage.getField());
            if (exists) {
                response.writeInteger(1);
            } else {
                response.writeInteger(0);
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}
