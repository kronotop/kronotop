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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.internal.NamespaceUtil;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.concurrent.CompletionException;

class RemoveSubcommand extends BaseSubcommand implements SubcommandExecutor {

    RemoveSubcommand(Context context) {
        super(context);
    }

    private void remove(String name, NamespaceMessage.RemoveMessage removeMessage) {
        // Remove namespaces by using an isolated, one-off transaction to prevent nasty consistency bugs.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NamespaceUtil.remove(tr, context.getClusterName(), removeMessage.getSubpath());
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(name);
            }
            if (e.getCause() instanceof FDBException ex) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                if (ex.getCode() == 1020) {
                    // retry
                    remove(name, removeMessage);
                    return;
                }
            }
            throw new KronotopException(e.getCause());
        }
    }

    @Override
    public void execute(Request request, Response response) {
        AsyncCommandExecutor.runAsync(context, response, () -> {
            NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
            NamespaceMessage.RemoveMessage removeMessage = message.getRemoveMessage();

            String name = String.join(".", removeMessage.getSubpath());
            if (context.getConfig().getString("default_namespace").equals(name)) {
                throw new KronotopException("Cannot remove the default namespace: '" + name + "'");
            }
            remove(name, removeMessage);
        }, response::writeOK);
    }
}
