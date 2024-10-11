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

import com.kronotop.common.KronotopException;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.generic.protocol.RenameNXMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(RenameNXMessage.COMMAND)
@MaximumParameterCount(RenameNXMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(RenameNXMessage.MINIMUM_PARAMETER_COUNT)
public class RenameNXHandler extends BaseGenericHandler implements Handler {
    public RenameNXHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.RENAMENX).get().getKey());
    }

    private int executeRenameNXCommand(RedisShard shard, RenameNXMessage message) {
        RedisValueContainer previous = shard.storage().get(message.getKey());
        if (previous == null) {
            throw new KronotopException("no such key");
        }

        if (shard.storage().containsKey(message.getNewkey())) {
            // newkey already exists.
            return 0;
        }

        shard.storage().remove(message.getKey());
        shard.storage().put(message.getNewkey(), previous);

        shard.index().add(message.getNewkey());
        syncMutatedStringOnVolume(shard, message.getNewkey(), previous.baseRedisValue().versionstamp());
        return 1;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.RENAMENX).set(new RenameNXMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        RenameNXMessage message = request.attr(MessageTypes.RENAMENX).get();

        List<String> keys = new ArrayList<>();
        keys.add(message.getKey());
        keys.add(message.getNewkey());

        RedisShard shard = service.findShard(keys);
        Iterable<ReadWriteLock> locks = shard.striped().bulkGet(keys);
        for (ReadWriteLock lock : locks) {
            lock.writeLock().lock();
        }
        int result;
        try {
            result = executeRenameNXCommand(shard, message);
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }
        response.writeInteger(result);
    }
}
