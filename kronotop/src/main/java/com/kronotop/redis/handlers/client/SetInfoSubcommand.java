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

package com.kronotop.redis.handlers.client;

import com.kronotop.common.KronotopException;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.cluster.protocol.ClusterMessage;
import com.kronotop.redis.handlers.client.protocol.ClientMessage;
import com.kronotop.redis.server.SubcommandExecutor;
import com.kronotop.server.*;

import java.util.HashMap;

public class SetInfoSubcommand implements SubcommandExecutor {
    private final RedisService service;

    public SetInfoSubcommand(RedisService service) {
        this.service = service;
    }

    @Override
    public void execute(Request request, Response response) {
        ClientMessage clientMessage = request.attr(MessageTypes.CLIENT).get();
        if (request.getParams().size() < 2) {
            // ERR wrong number of arguments for 'client|setinfo' command
            throw new WrongNumberOfArgumentsException(
                    String.format("wrong number of arguments for 'CLIENT|%s' command", clientMessage.getSubcommand())
            );
        }

        byte[] rawAttribute = new byte[request.getParams().get(1).readableBytes()];
        request.getParams().get(1).readBytes(rawAttribute);
        String attribute = new String(rawAttribute);

        HashMap<String, Object> channelAttributes = request.getChannelContext().channel().attr(ChannelAttributes.CLIENT_ATTRIBUTES).get();
        if (attribute.equalsIgnoreCase(Attribute.LIBNAME.toString())) {
            channelAttributes.put(Attribute.LIBNAME.toString(), attribute);
        } else if (attribute.equalsIgnoreCase(Attribute.LIBVER.toString())) {
            channelAttributes.put(Attribute.LIBVER.toString(), attribute);
        } else {
            throw new KronotopException(String.format("Unrecognized option '%s'", attribute));
        }

        response.writeOK();
    }

    enum Attribute {
        LIBNAME("lib-name"),
        LIBVER("lib-ver");

        private final String value;

        Attribute(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
}
