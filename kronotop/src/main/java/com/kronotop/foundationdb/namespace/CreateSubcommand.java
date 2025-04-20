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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


class CreateSubcommand extends BaseSubcommand implements SubcommandExecutor {

    CreateSubcommand(Context context) {
        super(context);
    }

    public void execute(Request request, Response response) {
        AsyncCommandExecutor.runAsync(context, response, () -> {
            NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();

            // Create the namespace by using an isolated, one-off transaction to prevent nasty consistency bugs.
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                CompletableFuture<DirectorySubspace> createFuture;
                List<String> subpath = getNamespaceSubpath(message.getCreateMessage().getSubpath());
                if (message.getCreateMessage().hasLayer() && message.getCreateMessage().hasPrefix()) {
                    createFuture = directoryLayer.create(tr, subpath, message.getCreateMessage().getLayer().getBytes(), message.getCreateMessage().getPrefix().getBytes());
                } else if (message.getCreateMessage().hasLayer()) {
                    createFuture = directoryLayer.create(tr, subpath, message.getCreateMessage().getLayer().getBytes());
                } else {
                    createFuture = directoryLayer.create(tr, subpath);
                }

                try {
                    createFuture.join();
                    tr.commit().join();
                } catch (CompletionException e) {
                    if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                        throw new NamespaceAlreadyExistsException(dottedNamespace(message.getCreateMessage().getSubpath()));
                    }
                    throw new KronotopException(e.getCause());
                }
            }
        }, response::writeOK);
    }
}
