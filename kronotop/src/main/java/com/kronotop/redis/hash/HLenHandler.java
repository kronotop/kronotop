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
import com.kronotop.redis.hash.protocol.HLenMessage;
import com.kronotop.redis.storage.Shard;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.concurrent.locks.ReadWriteLock;

@Command(HLenMessage.COMMAND)
@MinimumParameterCount(HLenMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HLenMessage.MAXIMUM_PARAMETER_COUNT)
public class HLenHandler extends BaseHandler implements Handler {
    public HLenHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HLEN).set(new HLenMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HLenMessage hlenMessage = request.attr(MessageTypes.HLEN).get();

        Shard shard = service.findShard(hlenMessage.getKey());
        ReadWriteLock lock = shard.getStriped().get(hlenMessage.getKey());
        lock.readLock().lock();
        try {
            Object retrieved = shard.get(hlenMessage.getKey());
            if (retrieved == null) {
                response.writeInteger(0);
                return;
            }
            if (!(retrieved instanceof HashValue)) {
                throw new WrongTypeException();
            }

            HashValue hashValue = (HashValue) retrieved;
            response.writeInteger(hashValue.size());
        } finally {
            lock.readLock().unlock();
        }
    }
}
