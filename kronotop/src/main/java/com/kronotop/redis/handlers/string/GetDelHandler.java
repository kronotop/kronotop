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
import com.kronotop.redis.handlers.string.protocol.GetDelMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.RedisValueKind;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.redis.RedisService.checkRedisValueKind;

@Command(GetDelMessage.COMMAND)
@MaximumParameterCount(GetDelMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(GetDelMessage.MINIMUM_PARAMETER_COUNT)
public class GetDelHandler extends BaseStringHandler implements Handler {
    public GetDelHandler(RedisService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.GETDEL).get().getKey());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.GETDEL).set(new GetDelMessage(request));
    }

    private RedisValueContainer executeGetDelCommand(RedisShard shard, GetDelMessage message) {
        RedisValueContainer previous = shard.storage().get(message.getKey());
        if (previous != null) {
            checkRedisValueKind(previous, RedisValueKind.STRING);
            shard.storage().remove(message.getKey());
        }
        return previous;
    }

    @Override
    public void execute(Request request, Response response) {
        GetDelMessage getDelMessage = request.attr(MessageTypes.GETDEL).get();

        RedisShard shard = service.findShard(getDelMessage.getKey(), ShardStatus.READWRITE);
        ReadWriteLock lock = shard.striped().get(getDelMessage.getKey());

        RedisValueContainer previous;
        try {
            lock.writeLock().lock();
            previous = executeGetDelCommand(shard, getDelMessage);
            if (previous != null) {
                deleteByVersionstamp(shard, previous.baseRedisValue().versionstamp());
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (previous == null) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }

        ByteBuf buf = response.getChannelContext().alloc().buffer();
        buf.writeBytes(previous.string().value());
        response.write(buf);
    }
}
