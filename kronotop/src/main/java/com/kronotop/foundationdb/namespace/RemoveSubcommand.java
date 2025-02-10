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

package com.kronotop.foundationdb.namespace;

import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.Context;
import com.kronotop.NamespaceUtils;
import com.kronotop.common.KronotopException;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.concurrent.CompletionException;

class RemoveSubcommand extends BaseSubcommand implements SubcommandExecutor {

    RemoveSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
        NamespaceMessage.RemoveMessage removeMessage = message.getRemoveMessage();

        String namespace = String.join(".", removeMessage.getSubpath());
        if (context.getConfig().getString("default_namespace").equals(namespace)) {
            throw new KronotopException("Cannot remove the default namespace: '" + namespace + "'");
        }

        // Remove namespaces by using an isolated, one-off transaction to prevent nasty consistency bugs.
        try {
            NamespaceUtils.remove(context, removeMessage.getSubpath());
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(String.join(".", removeMessage.getSubpath()));
            }
            throw new KronotopException(e.getCause());
        }
        response.writeOK();
    }
}
