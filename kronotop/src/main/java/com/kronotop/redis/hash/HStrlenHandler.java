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

import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.HashValue;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.hash.protocol.HStrlenMessage;
import com.kronotop.redis.storage.Partition;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

import java.util.concurrent.locks.ReadWriteLock;

@Command(HStrlenMessage.COMMAND)
@MinimumParameterCount(HStrlenMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HStrlenMessage.MAXIMUM_PARAMETER_COUNT)
public class HStrlenHandler extends BaseHandler implements Handler {
    public HStrlenHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HSTRLEN).set(new HStrlenMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HStrlenMessage hstrlenMessage = request.attr(MessageTypes.HSTRLEN).get();

        Partition partition = service.resolveKey(response.getContext(), hstrlenMessage.getKey());
        ReadWriteLock lock = partition.getStriped().get(hstrlenMessage.getKey());
        lock.readLock().lock();
        try {
            Object retrieved = partition.get(hstrlenMessage.getKey());
            if (retrieved == null) {
                response.writeInteger(0);
                return;
            }
            if (!(retrieved instanceof HashValue)) {
                throw new WrongTypeException();
            }

            HashValue hashValue = (HashValue) retrieved;
            byte[] value = hashValue.get(hstrlenMessage.getField());
            if (value != null) {
                response.writeInteger(value.length);
            } else {
                response.writeInteger(0);
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}
