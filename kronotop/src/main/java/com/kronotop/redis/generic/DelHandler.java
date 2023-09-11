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
import com.kronotop.redis.generic.protocol.DelMessage;
import com.kronotop.redis.storage.Partition;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

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
        DelMessage delMessage = request.attr(MessageTypes.DEL).get();

        Partition partition = service.resolveKeys(response.getContext(), delMessage.getKeys());

        Iterable<ReadWriteLock> locks = partition.getStriped().bulkGet(delMessage.getKeys());
        long keysRemoved = 0;
        try {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().lock();
            }

            for (String key : delMessage.getKeys()) {
                if (partition.remove(key) != null) {
                    keysRemoved++;
                    partition.getIndex().remove(key);
                }
            }
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }
        for (String key : delMessage.getKeys()) {
            partition.getPersistenceQueue().add(new StringKey(key));
        }
        response.writeInteger(keysRemoved);
    }
}
