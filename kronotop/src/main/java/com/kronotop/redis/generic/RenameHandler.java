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

package com.kronotop.redis.generic;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.generic.protocol.RenameMessage;
import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.storage.persistence.StringKey;
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

@Command(RenameMessage.COMMAND)
@MaximumParameterCount(RenameMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(RenameMessage.MINIMUM_PARAMETER_COUNT)
public class RenameHandler extends BaseGenericHandler implements Handler {
    public RenameHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.RENAME).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.RENAME).set(new RenameMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        RenameMessage renameMessage = request.attr(MessageTypes.RENAME).get();

        List<String> keys = new ArrayList<>();
        keys.add(renameMessage.getKey());
        keys.add(renameMessage.getNewkey());

        Shard shard = service.findShard(keys);
        Iterable<ReadWriteLock> locks = shard.getStriped().bulkGet(keys);
        try {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().lock();
            }
            Object result = shard.get(renameMessage.getKey());
            if (result == null) {
                response.writeError("no such key");
                return;
            }

            shard.put(renameMessage.getNewkey(), result);
            shard.getPersistenceQueue().add(new StringKey(renameMessage.getNewkey()));
            shard.remove(renameMessage.getKey(), result);
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }
        shard.getPersistenceQueue().add(new StringKey(renameMessage.getKey()));
        response.writeOK();
    }
}
