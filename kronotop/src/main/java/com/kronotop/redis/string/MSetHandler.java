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
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.string.protocol.MSetMessage;
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

@Command(MSetMessage.COMMAND)
@MinimumParameterCount(MSetMessage.MINIMUM_PARAMETER_COUNT)
public class MSetHandler extends BaseStringHandler implements Handler {
    public MSetHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.MSET).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.MSET).set(new MSetMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        MSetMessage msetMessage = request.attr(MessageTypes.MSET).get();

        RedisShard shard = service.findShard(msetMessage.getKeys());
        List<String> keys = new ArrayList<>();
        for (MSetMessage.Pair pair : msetMessage.getPairs()) {
            keys.add(pair.getKey());
        }

        Iterable<ReadWriteLock> locks = shard.striped().bulkGet(keys);
        try {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().lock();
            }
            for (MSetMessage.Pair pair : msetMessage.getPairs()) {
                RedisValueContainer previous = shard.storage().put(
                        pair.getKey(),
                        new RedisValueContainer(new StringValue(pair.getValue()))
                );
                syncStringOnVolume(shard, pair.getKey(), previous);
            }
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }
        response.writeOK();
    }
}
