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
import com.kronotop.redis.storage.Partition;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;

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
        String index = getCurrentLogicalDatabaseIndex(response.getContext());
        Collection<Partition> partitions = service.getLogicalDatabase(index).getPartitions().values();
        if (partitions.isEmpty()) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        List<Integer> partitionIds = new ArrayList<>();
        for (Partition partition : partitions) {
            partitionIds.add(partition.getId());
        }
        int randomIndex = ThreadLocalRandom.current().nextInt(partitionIds.size());
        int partitionId = partitionIds.get(randomIndex);
        Partition partition = service.getPartition(getCurrentLogicalDatabaseIndex(response.getContext()), partitionId);
        try {
            String randomKey = partition.getIndex().random();
            ByteBuf buf = response.getContext().alloc().buffer();
            buf.writeBytes(randomKey.getBytes());
            response.write(buf);
        } catch (NoSuchElementException e) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
        }
    }
}
