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

package com.kronotop.redis.string;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.Partition;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.redis.string.protocol.SetMessage;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(SetMessage.COMMAND)
@MaximumParameterCount(SetMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SetMessage.MINIMUM_PARAMETER_COUNT)
public class SetHandler extends BaseStringHandler implements Handler {
    public SetHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SET).set(new SetMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.SET).get().getKey());
    }

    @Override
    public void execute(Request request, Response response) {
        SetMessage setMessage = request.attr(MessageTypes.SET).get();

        Partition partition = service.resolveKey(response.getContext(), setMessage.getKey());
        StringValue stringValue = new StringValue(setMessage.getValue());
        ReadWriteLock lock = partition.getStriped().get(setMessage.getKey());
        try {
            lock.writeLock().lock();
            partition.put(setMessage.getKey(), stringValue);
            partition.getIndex().update(setMessage.getKey());
        } finally {
            lock.writeLock().unlock();
        }

        partition.getPersistenceQueue().add(new StringKey(setMessage.getKey()));
        response.writeOK();
    }
}
