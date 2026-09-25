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

package com.kronotop.namespace.handlers;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.journal.JournalName;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.namespace.NoSuchNamespaceException;
import com.kronotop.namespace.handlers.protocol.NamespaceSubcommand;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;

import java.util.List;
import java.util.concurrent.CompletionException;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class RemoveSubcommand extends BaseSubcommand implements SubcommandHandler {

    RemoveSubcommand(Context context) {
        super(context);
    }

    private void remove(String namespace, List<String> subpath) {
        // Remove namespaces by using an isolated, one-off transaction to prevent nasty consistency bugs.
        try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
            NamespaceMetadata metadata = NamespaceUtil.readMetadata(tr, context, subpath);
            if (metadata.removed()) {
                throw new NamespaceBeingRemovedException(namespace);
            }
            NamespaceUtil.setRemoved(tr, context, subpath);
            context.getJournal().getPublisher().publish(tr, JournalName.NAMESPACE_EVENTS, new NamespaceRemovedEvent(metadata.id(), namespace));
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(namespace);
            }
            if (e.getCause() instanceof FDBException ex) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                if (ex.getCode() == 1020) {
                    // retry
                    remove(namespace, subpath);
                    return;
                }
            }
            throw new KronotopException(e.getCause());
        }
    }

    @Override
    public void execute(Request request, Response response) {
        RemoveParameters parameters = new RemoveParameters(request);
        runAsync(context, response, () -> {
            String name = String.join(".", parameters.subpath);
            if (context.getConfig().getString("default_namespace").equals(name)) {
                throw new KronotopException("Cannot remove the default namespace: '" + name + "'");
            }
            remove(name, parameters.subpath);
        }, response::writeOK);
    }

    private class RemoveParameters {
        private final List<String> subpath;

        private RemoveParameters(Request request) {
            if (request.getParams().size() != 2) {
                throw wrongNumberOfArguments(request, NamespaceSubcommand.REMOVE);
            }
            subpath = readSubpath(request.getParams().get(1));
            validateSubpath(subpath);
        }
    }
}
