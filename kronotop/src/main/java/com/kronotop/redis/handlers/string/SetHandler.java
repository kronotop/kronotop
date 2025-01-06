/*
 * Copyright (c) 2023-2025 Kronotop
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
import com.kronotop.redis.handlers.string.protocol.SetMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

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

    private RedisValueContainer executeSetCommand(RedisShard shard, SetMessage message) {
        StringValue stringValue = new StringValue(message.getValue());
        RedisValueContainer container = new RedisValueContainer(stringValue);
        return shard.storage().put(message.getKey(), container);
    }

    @Override
    public void execute(Request request, Response response) {
        SetMessage message = request.attr(MessageTypes.SET).get();

        RedisShard shard = service.findShard(message.getKey(), ShardStatus.READWRITE);
        ReadWriteLock lock = shard.striped().get(message.getKey());
        lock.writeLock().lock();
        try {
            RedisValueContainer previous = executeSetCommand(shard, message);
            syncStringOnVolume(shard, message.getKey(), previous);
        } finally {
            lock.writeLock().unlock();
        }
        response.writeOK();
    }
}
