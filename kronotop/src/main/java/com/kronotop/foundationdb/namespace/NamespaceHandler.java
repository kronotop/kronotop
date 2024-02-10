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

package com.kronotop.foundationdb.namespace;

import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.foundationdb.namespace.protocol.NamespaceSubcommand;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.EnumMap;

@Command(NamespaceMessage.COMMAND)
@MinimumParameterCount(NamespaceMessage.MINIMUM_PARAMETER_COUNT)
public class NamespaceHandler implements Handler {
    private final EnumMap<NamespaceSubcommand, NamespaceSubcommandExecutor> executors = new EnumMap<>(NamespaceSubcommand.class);

    public NamespaceHandler(FoundationDBService service) {
        executors.put(NamespaceSubcommand.CREATE, new NamespaceCreateSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.CURRENT, new NamespaceCurrentSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.EXISTS, new NamespaceExistsSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.LIST, new NamespaceListSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.MOVE, new NamespaceMoveSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.REMOVE, new NamespaceRemoveSubcommand(service.getContext()));
        executors.put(NamespaceSubcommand.USE, new NamespaceUseSubcommand(service.getContext()));
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.NAMESPACE).set(new NamespaceMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        NamespaceMessage namespaceMessage = request.attr(MessageTypes.NAMESPACE).get();

        NamespaceSubcommandExecutor executor = executors.get(namespaceMessage.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(namespaceMessage.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
