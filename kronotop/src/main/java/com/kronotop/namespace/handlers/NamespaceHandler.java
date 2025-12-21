/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.namespace.handlers;

import com.kronotop.namespace.NamespaceService;
import com.kronotop.namespace.handlers.protocol.NamespaceMessage;
import com.kronotop.namespace.handlers.protocol.NamespaceSubcommand;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.EnumMap;

@Command(NamespaceMessage.COMMAND)
@MinimumParameterCount(NamespaceMessage.MINIMUM_PARAMETER_COUNT)
public class NamespaceHandler implements Handler {
    private final EnumMap<NamespaceSubcommand, SubcommandExecutor> executors = new EnumMap<>(NamespaceSubcommand.class);

    public NamespaceHandler(NamespaceService service) {
        executors.put(NamespaceSubcommand.CREATE, new CreateSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.CURRENT, new CurrentSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.EXISTS, new ExistsSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.LIST, new ListSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.MOVE, new MoveSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.REMOVE, new RemoveSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.PURGE, new PurgeSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.USE, new UseSubcommand(service.getContext()));
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.NAMESPACE).set(new NamespaceMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();

        SubcommandExecutor executor = executors.get(message.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
