/*
 * Copyright (c) 2023-2025 Kronotop
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

import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.handlers.client.protocol.ClientMessage;
import com.kronotop.redis.handlers.client.protocol.ClientSubcommand;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.EnumMap;

@Command(ClientMessage.COMMAND)
@MinimumParameterCount(ClientMessage.MINIMUM_PARAMETER_COUNT)
public class ClientHandler extends BaseHandler implements Handler {
    private final EnumMap<ClientSubcommand, SubcommandHandler> executors = new EnumMap<>(ClientSubcommand.class);


    public ClientHandler(RedisService service) {
        super(service);

        executors.put(ClientSubcommand.SETINFO, new SetInfoSubcommand());
        executors.put(ClientSubcommand.SETNAME, new SetNameSubcommand());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.CLIENT).set(new ClientMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        ClientMessage clientMessage = request.attr(MessageTypes.CLIENT).get();
        SubcommandHandler executor = executors.get(clientMessage.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(clientMessage.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
