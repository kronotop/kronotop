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

package com.kronotop.redis.transactions;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.transactions.protocol.ExecMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

@Command(ExecMessage.COMMAND)
@MaximumParameterCount(ExecMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ExecMessage.MINIMUM_PARAMETER_COUNT)
public class ExecHandler implements Handler {
    private final RedisService service;

    public ExecHandler(RedisService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.EXEC).set(new ExecMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        Channel channel = response.getChannelContext().channel();
        Attribute<Boolean> redisMulti = channel.attr(ChannelAttributes.REDIS_MULTI);
        if (!Boolean.TRUE.equals(redisMulti.get())) {
            response.writeError("EXEC without MULTI");
            return;
        }

        // Impossible!
        response.writeError("There is a critical bug in Redis transaction handling. Please report this.");
    }
}

