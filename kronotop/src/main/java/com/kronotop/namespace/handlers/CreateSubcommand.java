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
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.namespace.NamespaceAlreadyExistsException;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.namespace.TombstoneManager;
import com.kronotop.namespace.handlers.protocol.NamespaceSubcommand;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;

import java.util.List;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class CreateSubcommand extends BaseSubcommand implements SubcommandHandler {
    private static final String IF_NOT_EXISTS = "IF-NOT-EXISTS";

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
            } catch (NamespaceAlreadyExistsException e) {
                if (parameters.ifNotExists) {
                    // Namespace exists and is not being removed, ready to use.
                    return;
                }
                throw e;
            }
        }, response::writeOK);
    }

    private class CreateParameters {
        private final List<String> subpath;
        private boolean ifNotExists;

        private CreateParameters(Request request) {
            int size = request.getParams().size();
            if (size < 2 || size > 3) {
                throw wrongNumberOfArguments(request, NamespaceSubcommand.CREATE);
            }
            subpath = readSubpath(request.getParams().get(1));
            validateSubpath(subpath);

            if (size == 3) {
                String raw = ProtocolMessageUtil.readAsString(request.getParams().get(2));
                if (!StringUtil.toUpperCaseAscii(raw).equals(IF_NOT_EXISTS)) {
                    throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
                }
                ifNotExists = true;
            }
        }
    }
}
