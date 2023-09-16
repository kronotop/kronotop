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

package com.kronotop.redis.generic;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.generic.protocol.RenameMessage;
import com.kronotop.redis.storage.Partition;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

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

        Partition firstPartition = service.resolveKey(response.getContext(), renameMessage.getKey());
        Partition secondPartition = service.resolveKey(response.getContext(), renameMessage.getNewkey());

        List<String> keys = new ArrayList<>();
        keys.add(renameMessage.getKey());
        keys.add(renameMessage.getNewkey());

        Iterable<ReadWriteLock> locks = firstPartition.getStriped().bulkGet(keys);
        try {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().lock();
            }
            Object result = firstPartition.get(renameMessage.getKey());
            if (result == null) {
                response.writeError("no such key");
                return;
            }

            // Add the new key with the existing value
            secondPartition.put(renameMessage.getNewkey(), result);
            secondPartition.getIndex().add(renameMessage.getNewkey());
            secondPartition.getPersistenceQueue().add(new StringKey(renameMessage.getNewkey()));

            // Cleanup
            firstPartition.remove(renameMessage.getKey(), result);
            firstPartition.getIndex().remove(renameMessage.getKey());
            firstPartition.getPersistenceQueue().add(new StringKey(renameMessage.getKey()));
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }
        response.writeOK();
    }
}
