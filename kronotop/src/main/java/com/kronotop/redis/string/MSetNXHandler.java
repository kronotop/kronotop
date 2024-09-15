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

package com.kronotop.redis.string;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.persistence.RedisValueContainer;
import com.kronotop.redis.storage.persistence.jobs.AppendStringJob;
import com.kronotop.redis.string.protocol.MSetNXMessage;
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

    private int msetnx(RedisShard shard, MSetNXMessage mSetNXMessage) {
        for (MSetNXMessage.Pair pair : mSetNXMessage.getPairs()) {
            if (shard.storage().containsKey(pair.getKey())) {
                return 0;
            }
        }
        for (MSetNXMessage.Pair pair : mSetNXMessage.getPairs()) {
            RedisValueContainer previousValue = shard.storage().put(
                    pair.getKey(),
                    new RedisValueContainer(new StringValue(pair.getValue()))
            );
            if (previousValue == null) {
                shard.index().add(pair.getKey());
            }
        }
        return 1;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.MSETNX).set(new MSetNXMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        MSetNXMessage msetnxMessage = request.attr(MessageTypes.MSETNX).get();

        List<String> keys = new ArrayList<>();
        for (MSetNXMessage.Pair pair : msetnxMessage.getPairs()) {
            keys.add(pair.getKey());
        }

        RedisShard shard = service.findShard(msetnxMessage.getKeys());
        int result;
        Iterable<ReadWriteLock> locks = shard.striped().bulkGet(keys);
        try {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().lock();
            }
            result = msetnx(shard, msetnxMessage);
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }

        for (String key : msetnxMessage.getKeys()) {
            shard.persistenceQueue().add(new AppendStringJob(key));
        }
        response.writeInteger(result);
    }
}
