/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.handlers.generic.protocol.ExistsMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.concurrent.locks.ReadWriteLock;

@Command(ExistsMessage.COMMAND)
@MinimumParameterCount(ExistsMessage.MINIMUM_PARAMETER_COUNT)
public class ExistsHandler extends BaseHandler implements Handler {
    public ExistsHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.EXISTS).set(new ExistsMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        ExistsMessage existsMessage = request.attr(MessageTypes.EXISTS).get();

        RedisShard shard = service.findShard(existsMessage.getKeys(), ShardStatus.READONLY);

        Iterable<ReadWriteLock> locks = shard.striped().bulkGet(existsMessage.getKeys());
        for (ReadWriteLock lock : locks) {
            lock.readLock().lock();
        }

        long total = 0;
        try {
            for (String key : existsMessage.getKeys()) {
                if (shard.storage().containsKey(key)) {
                    total++;
                }
            }
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.readLock().unlock();
            }
        }
        response.writeInteger(total);
    }
}
