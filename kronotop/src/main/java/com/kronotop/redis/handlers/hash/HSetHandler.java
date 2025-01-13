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

package com.kronotop.redis.handlers.hash;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.hash.protocol.FieldValuePair;
import com.kronotop.redis.handlers.hash.protocol.HSetMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.RedisValueKind;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.redis.RedisService.checkRedisValueKind;

@Command(HSetMessage.COMMAND)
@MinimumParameterCount(HSetMessage.MINIMUM_PARAMETER_COUNT)
public class HSetHandler extends BaseHashHandler implements Handler {
    public HSetHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HSET).set(new HSetMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.HSET).get().getKey());
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HSetMessage message = request.attr(MessageTypes.HSET).get();

        RedisShard shard = service.findShard(message.getKey(), ShardStatus.READWRITE);
        ReadWriteLock lock = shard.striped().get(message.getKey());
        lock.writeLock().lock();
        int total = 0;
        try {
            HashValue hashValue;
            RedisValueContainer previous = shard.storage().get(message.getKey());
            if (previous == null) {
                hashValue = new HashValue();
                shard.storage().put(message.getKey(), new RedisValueContainer(hashValue));
            } else {
                checkRedisValueKind(previous, RedisValueKind.HASH);
                hashValue = previous.hash();
            }

            for (FieldValuePair fieldValuePair : message.getFieldValuePairs()) {
                HashFieldValue previousHashField = hashValue.put(fieldValuePair.getField(), fieldValuePair.getValue());
                if (previousHashField != null) {
                    deleteByVersionstamp(shard, previousHashField);
                } else {
                    total++;
                }
                syncHashField(shard, message.getKey(), fieldValuePair);
            }
        } finally {
            lock.writeLock().unlock();
        }
        response.writeInteger(total);
    }
}
