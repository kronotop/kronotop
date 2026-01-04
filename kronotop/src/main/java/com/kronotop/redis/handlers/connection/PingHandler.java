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

import com.kronotop.redis.handlers.connection.protocol.PingMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

@Command(PingMessage.COMMAND)
@MaximumParameterCount(PingMessage.MAXIMUM_PARAMETER_COUNT)
public class PingHandler implements Handler {
    @Override
    public boolean requiresClusterInitialization() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.PING).set(new PingMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        PingMessage pingMessage = request.attr(MessageTypes.PING).get();
        if (pingMessage.getMessage() != null) {
            if (!pingMessage.getMessage().isEmpty() || !pingMessage.getMessage().isBlank()) {
                ByteBuf buf = Unpooled.wrappedBuffer(pingMessage.getMessage().getBytes(StandardCharsets.UTF_8));
                response.writeFullBulkString(new FullBulkStringRedisMessage(buf));
                return;
            }
        }
        response.writeSimpleString(Response.PONG);
    }
}
