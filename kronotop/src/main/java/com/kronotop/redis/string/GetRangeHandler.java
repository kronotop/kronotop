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
import com.kronotop.redis.ResolveResponse;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.string.protocol.GetRangeMessage;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;

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
        ByteBuf buf = response.getContext().alloc().buffer();
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

        ResolveResponse resolveResponse = service.resolveKey(getRangeMessage.getKey());
        if (resolveResponse.hasError()) {
            response.writeError(resolveResponse.getError());
            return;
        }

        LogicalDatabase storage = getLogicalDatabase(response.getContext());
        Object result;
        ReadWriteLock lock = storage.getStriped().get(getRangeMessage.getKey());

        try {
            lock.readLock().lock();
            result = storage.get(getRangeMessage.getKey());
        } finally {
            lock.readLock().unlock();
        }

        if (result == null) {
            emptyResponse(response);
            return;
        }

        StringValue value = (StringValue) result;

        int start = getRangeMessage.getStart();
        int end = getRangeMessage.getEnd();
        if (end > 0) {
            end += 1;
        }

        if (start == 0 && end == -1) {
            end = value.getValue().length;
        }

        if (start < 0) {
            start = value.getValue().length + start;
        }

        if (end < 0) {
            end = value.getValue().length + end + 1;
        }

        if (end > value.getValue().length) {
            end = value.getValue().length;
        }

        try {
            byte[] data = Arrays.copyOfRange(value.getValue(), start, end);
            ByteBuf buf = response.getContext().alloc().buffer();
            buf.writeBytes(data);
            response.writeFullBulkString(new FullBulkStringRedisMessage(buf));
        } catch (IllegalArgumentException e) {
            emptyResponse(response);
        }
    }

}
