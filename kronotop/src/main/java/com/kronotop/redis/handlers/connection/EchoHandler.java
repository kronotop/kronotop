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

package com.kronotop.redis.handlers.connection;

import com.kronotop.redis.handlers.connection.protocol.EchoMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

@Command(EchoMessage.COMMAND)
@MaximumParameterCount(EchoMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(EchoMessage.MINIMUM_PARAMETER_COUNT)
public class EchoHandler implements Handler {

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ECHO).set(new EchoMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        EchoMessage message = request.attr(MessageTypes.ECHO).get();
        ByteBuf buf = response.getCtx().alloc().buffer();
        buf.writeBytes(message.getMessage().getBytes(StandardCharsets.UTF_8));
        response.writeFullBulkString(new FullBulkStringRedisMessage(buf));
    }
}
