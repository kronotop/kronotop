/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.core.handlers.server;

import com.kronotop.Context;
import com.kronotop.core.handlers.server.protocol.CommandMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.resp3.RedisMessage;

import java.util.ArrayList;
import java.util.List;

@Command(CommandMessage.COMMAND)
@MaximumParameterCount(CommandMessage.MAXIMUM_PARAMETER_COUNT)
public class CommandHandler implements Handler {
    private final Context context;

    public CommandHandler(Context context) {
        this.context = context;
    }

    @Override
    public boolean requiresClusterInitialization() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.COMMAND).set(new CommandMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        List<RedisMessage> root = new ArrayList<>();
        CommandMessage message = request.attr(MessageTypes.COMMAND).get();
        response.writeArray(root);
    }
}
