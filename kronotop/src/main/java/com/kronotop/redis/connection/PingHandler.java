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

package com.kronotop.redis.connection;

import com.kronotop.redis.connection.protocol.PingMessage;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;

import java.nio.charset.StandardCharsets;

@Command(PingMessage.COMMAND)
@MaximumParameterCount(PingMessage.MAXIMUM_PARAMETER_COUNT)
public class PingHandler implements Handler {
    final String DEFAULT_PING_RESPONSE = "PONG";

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.PING).set(new PingMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        PingMessage pingMessage = request.attr(MessageTypes.PING).get();
        if (pingMessage.getMessage() != null) {
            if (!pingMessage.getMessage().isEmpty() || !pingMessage.getMessage().isBlank()) {
                ByteBuf buf = response.getContext().alloc().buffer();
                buf.writeBytes(pingMessage.getMessage().getBytes(StandardCharsets.UTF_8));
                response.writeFullBulkString(new FullBulkStringRedisMessage(buf));
                return;
            }
        }
        response.writeSimpleString(DEFAULT_PING_RESPONSE);
    }
}
