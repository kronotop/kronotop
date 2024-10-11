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

package com.kronotop.redis.handlers.generic;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.generic.protocol.DelMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(DelMessage.COMMAND)
@MinimumParameterCount(DelMessage.MINIMUM_PARAMETER_COUNT)
public class DelHandler extends BaseGenericHandler implements Handler {
    public DelHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.DEL).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.DEL).set(new DelMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        DelMessage message = request.attr(MessageTypes.DEL).get();

        RedisShard shard = service.findShard(message.getKeys());

        Iterable<ReadWriteLock> locks = shard.striped().bulkGet(message.getKeys());
        long keysRemoved = 0;
        for (ReadWriteLock lock : locks) {
            lock.writeLock().lock();
        }
        try {
            for (String key : message.getKeys()) {
                RedisValueContainer container = shard.storage().remove(key);
                if (container != null) {
                    keysRemoved++;
                    wipeOutKey(shard, key, container);
                }
            }
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }
        response.writeInteger(keysRemoved);
    }
}
