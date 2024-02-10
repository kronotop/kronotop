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

import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.generic.protocol.RandomKeyMessage;
import com.kronotop.redis.storage.Shard;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

@Command(RandomKeyMessage.COMMAND)
@MaximumParameterCount(RandomKeyMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(RandomKeyMessage.MINIMUM_PARAMETER_COUNT)
public class RandomKeyHandler extends BaseHandler implements Handler {
    public RandomKeyHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.RANDOMKEY).set(new RandomKeyMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        Collection<Shard> shards = service.getContext().getLogicalDatabase().getShards().values();
        if (shards.isEmpty()) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        List<Integer> shardIds = new ArrayList<>();
        for (Shard shard : shards) {
            if (!shard.isEmpty()) {
                shardIds.add(shard.getId());
            }
        }

        if (shardIds.isEmpty()) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }

        int randomIndex = ThreadLocalRandom.current().nextInt(shardIds.size());
        int shardId = shardIds.get(randomIndex);
        Shard shard = service.getShard(shardId);
        try {
            String randomKey = shard.getIndex().random();
            ByteBuf buf = response.getChannelContext().alloc().buffer();
            buf.writeBytes(randomKey.getBytes());
            response.write(buf);
        } catch (NoSuchElementException e) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
        }
    }
}
