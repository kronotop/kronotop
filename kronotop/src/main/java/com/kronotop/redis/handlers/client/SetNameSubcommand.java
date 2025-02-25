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

package com.kronotop.redis.handlers.client;

import com.kronotop.redis.handlers.client.protocol.ClientMessage;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.*;

import java.util.HashMap;

public class SetNameSubcommand implements SubcommandHandler {

    @Override
    public void execute(Request request, Response response) {
        if (request.getParams().size() != 2) {
            ClientMessage clientMessage = request.attr(MessageTypes.CLIENT).get();
            // ERR wrong number of arguments for 'client|setinfo' command
            throw new WrongNumberOfArgumentsException(
                    String.format("wrong number of arguments for 'CLIENT|%s' command", clientMessage.getSubcommand())
            );
        }

        byte[] rawName = new byte[request.getParams().get(1).readableBytes()];
        request.getParams().get(1).readBytes(rawName);
        String name = new String(rawName);

        HashMap<String, Object> channelAttributes = request.getSession().attr(SessionAttributes.CLIENT_ATTRIBUTES).get();
        channelAttributes.put("name", name);

        response.writeOK();
    }
}
