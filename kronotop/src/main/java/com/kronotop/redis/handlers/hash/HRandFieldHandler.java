/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.redis.handlers.hash;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.handlers.hash.protocol.HRandFieldMessage;
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
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.redis.RedisService.checkRedisValueKind;

@Command(HRandFieldMessage.COMMAND)
@MinimumParameterCount(HRandFieldMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HRandFieldMessage.MAXIMUM_PARAMETER_COUNT)
public class HRandFieldHandler extends BaseHandler implements Handler {
    private final Random random;

    public HRandFieldHandler(RedisService service) {
        super(service);
        random = new Random();
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HRANDFIELD).set(new HRandFieldMessage(request));
    }

    private <E> E getRandomSetElement(Set<E> set) {
        return set.stream().skip(random.nextInt(set.size())).findFirst().orElse(null);
    }

    private FullBulkStringRedisMessage prepareBulkReply(Response response, HashValue hashValue) {
        String field = getRandomSetElement(hashValue.keySet());
        ByteBuf buf = response.getCtx().alloc().buffer();
        buf.writeBytes(field.getBytes());
        return new FullBulkStringRedisMessage(buf);
    }

    private List<RedisMessage> prepareArrayReply(Response response, HRandFieldMessage hrandfieldMessage, HashValue hashValue) {
        int count = Math.abs(hrandfieldMessage.getCount());
        if (count > hashValue.size()) {
            count = hashValue.size();
        }

        Set<String> set = new HashSet<>();
        List<RedisMessage> upperList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String field = getRandomSetElement(hashValue.keySet());
            if (hrandfieldMessage.getCount() > 0) {
                if (set.contains(field)) {
                    i--;
                    continue;
                }
            }
            set.add(field);
            ByteBuf fieldBuf = response.getCtx().alloc().buffer();
            fieldBuf.writeBytes(field.getBytes());
            upperList.add(new FullBulkStringRedisMessage(fieldBuf));

            if (hrandfieldMessage.getWithValues()) {
                ByteBuf valueBuf = response.getCtx().alloc().buffer();
                valueBuf.writeBytes(hashValue.get(field).value());
                upperList.add(new FullBulkStringRedisMessage(valueBuf));
            }

        }
        return upperList;
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HRandFieldMessage hrandfieldMessage = request.attr(MessageTypes.HRANDFIELD).get();

        FullBulkStringRedisMessage bulkReply = null;
        List<RedisMessage> arrayReply = null;

        RedisShard shard = service.findShard(hrandfieldMessage.getKey(), ShardStatus.READONLY);
        ReadWriteLock lock = shard.striped().get(hrandfieldMessage.getKey());
        lock.readLock().lock();
        try {
            RedisValueContainer container = shard.storage().get(hrandfieldMessage.getKey());
            if (container == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }
            checkRedisValueKind(container, RedisValueKind.HASH);

            if (hrandfieldMessage.getCount() == null) {
                bulkReply = prepareBulkReply(response, container.hash());
            } else {
                arrayReply = prepareArrayReply(response, hrandfieldMessage, container.hash());
            }
        } finally {
            lock.readLock().unlock();
        }

        if (bulkReply != null) {
            response.writeFullBulkString(bulkReply);
        } else {
            response.writeArray(arrayReply);
        }
    }
}
