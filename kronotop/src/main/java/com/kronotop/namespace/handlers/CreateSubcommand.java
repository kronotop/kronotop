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
import com.kronotop.Context;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.namespace.TombstoneManager;
import com.kronotop.namespace.handlers.protocol.NamespaceSubcommand;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class CreateSubcommand extends BaseSubcommand implements SubcommandHandler {

    CreateSubcommand(Context context) {
        super(context);
    }

    public void execute(Request request, Response response) {
        CreateParameters parameters = new CreateParameters(request);
        runAsync(context, response, () -> {
            String namespace = dottedNamespace(parameters.subpath);

            // Create the namespace by using an isolated, one-off transaction to prevent nasty consistency bugs.
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                TombstoneManager.checkBarrier(context, tr, namespace);
                // Commits the transaction itself.
                NamespaceUtil.create(context, tr, parameters.subpath);
            }
        }, response::writeOK);
    }

    private class CreateParameters {
        private final List<String> subpath = new ArrayList<>();

        private CreateParameters(Request request) {
            ListIterator<ByteBuf> iterator = request.getParams().listIterator(1);
            while (iterator.hasNext()) {
                ByteBuf rawItem = iterator.next();
                if (!subpath.isEmpty()) {
                    throw wrongNumberOfArguments(request, NamespaceSubcommand.CREATE);
                }
                subpath.addAll(readSubpath(rawItem));
                validateSubpath(subpath);
            }

            if (subpath.isEmpty()) {
                throw wrongNumberOfArguments(request, NamespaceSubcommand.CREATE);
            }
        }
    }
}
