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

package com.kronotop.redis.handlers.string;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.string.protocol.MSetNXMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(MSetNXMessage.COMMAND)
@MinimumParameterCount(MSetNXMessage.MINIMUM_PARAMETER_COUNT)
public class MSetNXHandler extends BaseStringHandler implements Handler {
    public MSetNXHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.MSETNX).get().getKey());
    }

    private int executeMSetNXCommand(RedisShard shard, MSetNXMessage message) {
        for (MSetNXMessage.Pair pair : message.getPairs()) {
            if (shard.storage().containsKey(pair.getKey())) {
                return 0;
            }
        }
        for (MSetNXMessage.Pair pair : message.getPairs()) {
            RedisValueContainer previous = shard.storage().put(
                    pair.getKey(),
                    new RedisValueContainer(new StringValue(pair.getValue()))
            );
            syncStringOnVolume(shard, pair.getKey(), previous);
        }
        return 1;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.MSETNX).set(new MSetNXMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        MSetNXMessage message = request.attr(MessageTypes.MSETNX).get();

        List<String> keys = new ArrayList<>();
        for (MSetNXMessage.Pair pair : message.getPairs()) {
            keys.add(pair.getKey());
        }

        RedisShard shard = service.findShard(message.getKeys(), ShardStatus.READWRITE);
        int result;
        Iterable<ReadWriteLock> locks = shard.striped().bulkGet(keys);
        for (ReadWriteLock lock : locks) {
            lock.writeLock().lock();
        }
        try {
            result = executeMSetNXCommand(shard, message);
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }
        response.writeInteger(result);
    }
}
