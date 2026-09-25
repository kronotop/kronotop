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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.journal.JournalName;
import com.kronotop.namespace.NamespaceAlreadyExistsException;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.namespace.NoSuchNamespaceException;
import com.kronotop.namespace.TombstoneManager;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.namespace.handlers.protocol.NamespaceSubcommand;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;

import java.util.List;
import java.util.concurrent.CompletionException;

class MoveSubcommand extends BaseSubcommand implements SubcommandHandler {

    MoveSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        MoveParameters parameters = new MoveParameters(request);
        AsyncCommandExecutor.runAsync(context, response, () -> {
            List<String> oldPath = getNamespaceSubpath(parameters.oldPath);
            List<String> newPath = getNamespaceSubpath(parameters.newPath);

            // Move namespaces by using an isolated, one-off transaction to prevent nasty consistency bugs.
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                checkNamespaceBeingRemoved(tr, parameters.oldPath);
                DirectorySubspace newSubspace = context.getDirectoryLayer().move(tr, oldPath, newPath).join();
                NamespaceUtil.updateLeafAndParentPointer(context, tr, newSubspace, parameters.newPath);

                String oldNamespace = String.join(".", parameters.oldPath);
                String token = TombstoneManager.setTombstone(context, tr, oldNamespace);
                context.getJournal().getPublisher().publish(
                        tr,
                        JournalName.NAMESPACE_EVENTS,
                        new NamespaceMovedEvent(oldNamespace, token)
                );

                tr.commit().join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof NoSuchDirectoryException) {
                    throw new NoSuchNamespaceException(String.join(".", parameters.oldPath));
                } else if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                    throw new NamespaceAlreadyExistsException(dottedNamespace(parameters.newPath));
                }
                throw new KronotopException(e.getCause());
            }
        }, response::writeOK);
    }

    private class MoveParameters {
        private final List<String> oldPath;
        private final List<String> newPath;

        private MoveParameters(Request request) {
            if (request.getParams().size() != 3) {
                throw wrongNumberOfArguments(request, NamespaceSubcommand.MOVE);
            }
            oldPath = readSubpath(request.getParams().get(1));
            newPath = readSubpath(request.getParams().get(2));
            validateSubpath(oldPath);
            validateSubpath(newPath);
        }
    }
}
