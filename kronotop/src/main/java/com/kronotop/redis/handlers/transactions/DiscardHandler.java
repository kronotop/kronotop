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

package com.kronotop.redis.handlers.transactions;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.transactions.protocol.DiscardMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;

@Command(DiscardMessage.COMMAND)
@MaximumParameterCount(DiscardMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(DiscardMessage.MINIMUM_PARAMETER_COUNT)
public class DiscardHandler implements Handler {
    private final RedisService service;

    public DiscardHandler(RedisService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.DISCARD).set(new DiscardMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        ChannelHandlerContext ctx = response.getChannelContext();
        Attribute<Boolean> redisMulti = ctx.channel().attr(ChannelAttributes.REDIS_MULTI);
        if (!Boolean.TRUE.equals(redisMulti.get())) {
            response.writeError("DISCARD without MULTI");
            return;
        }
        service.cleanupRedisTransaction(ctx);
        response.writeOK();
    }
}

