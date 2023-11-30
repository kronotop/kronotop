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

import com.kronotop.common.KronotopException;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.generic.protocol.RenameNXMessage;
import com.kronotop.redis.storage.Shard;
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

    private int renamenx(Response response, RenameNXMessage renamenxMessage) {
        List<String> keys = new ArrayList<>();
        keys.add(renamenxMessage.getKey());
        keys.add(renamenxMessage.getNewkey());

        Shard shard = service.resolveKeys(response.getContext(), keys);
        Iterable<ReadWriteLock> locks = shard.getStriped().bulkGet(keys);
        try {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().lock();
            }

            Object result = shard.get(renamenxMessage.getKey());
            if (result == null) {
                throw new KronotopException("no such key");
            }

            if (shard.containsKey(renamenxMessage.getNewkey())) {
                // newkey already exists.
                return 0;
            }

            shard.put(renamenxMessage.getNewkey(), result);
            shard.getPersistenceQueue().add(new StringKey(renamenxMessage.getNewkey()));
            shard.remove(renamenxMessage.getKey(), result);
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.writeLock().unlock();
            }
        }

        shard.getPersistenceQueue().add(new StringKey(renamenxMessage.getKey()));
        return 1;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.RENAMENX).set(new RenameNXMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        RenameNXMessage renamenxMessage = request.attr(MessageTypes.RENAMENX).get();

        int result;
        try {
            result = renamenx(response, renamenxMessage);
        } catch (KronotopException e) {
            response.writeError(e.getMessage());
            return;
        }

        response.writeInteger(result);
    }
}
