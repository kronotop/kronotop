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

package com.kronotop.redis.handlers.string;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.string.protocol.GetRangeMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;

@Command(GetRangeMessage.COMMAND)
@MaximumParameterCount(GetRangeMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(GetRangeMessage.MINIMUM_PARAMETER_COUNT)
public class GetRangeHandler extends BaseStringHandler implements Handler {
    public GetRangeHandler(RedisService service) {
        super(service);
    }

    private void emptyResponse(Response response) {
        ByteBuf buf = response.getChannelContext().alloc().buffer();
        buf.writeBytes("".getBytes());
        response.writeFullBulkString(new FullBulkStringRedisMessage(buf));
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.GETRANGE).set(new GetRangeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        GetRangeMessage getRangeMessage = request.attr(MessageTypes.GETRANGE).get();

        RedisShard shard = service.findShard(getRangeMessage.getKey());
        RedisValueContainer container;
        ReadWriteLock lock = shard.striped().get(getRangeMessage.getKey());

        try {
            lock.readLock().lock();
            container = shard.storage().get(getRangeMessage.getKey());
        } finally {
            lock.readLock().unlock();
        }

        if (container == null) {
            emptyResponse(response);
            return;
        }

        StringValue value = container.string();

        int start = getRangeMessage.getStart();
        int end = getRangeMessage.getEnd();
        if (end > 0) {
            end += 1;
        }

        if (start == 0 && end == -1) {
            end = value.value().length;
        }

        if (start < 0) {
            start = value.value().length + start;
        }

        if (end < 0) {
            end = value.value().length + end + 1;
        }

        if (end > value.value().length) {
            end = value.value().length;
        }

        try {
            byte[] data = Arrays.copyOfRange(value.value(), start, end);
            ByteBuf buf = response.getChannelContext().alloc().buffer();
            buf.writeBytes(data);
            response.writeFullBulkString(new FullBulkStringRedisMessage(buf));
        } catch (IllegalArgumentException e) {
            emptyResponse(response);
        }
    }

}
