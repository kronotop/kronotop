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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.namespace.NamespaceVersionBarrier;
import com.kronotop.namespace.handlers.protocol.NamespaceSubcommand;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;

import java.time.Duration;
import java.util.List;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class PurgeSubcommand extends BaseSubcommand implements SubcommandHandler {

    PurgeSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        PurgeParameters parameters = new PurgeParameters(request);
        runAsync(context, response, () -> {
            String name = String.join(".", parameters.subpath);
            if (context.getConfig().getString("default_namespace").equals(name)) {
                throw new KronotopException("Cannot purge the default namespace: '" + name + "'");
            }

            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                DirectorySubspace subspace = NamespaceUtil.open(tr, context, parameters.subpath);
                NamespaceMetadata metadata = NamespaceUtil.readMetadata(tr, name, subspace);
                if (!metadata.removed()) {
                    throw new KronotopException(
                            String.format("Namespace '%s' must be logically removed before purge", dottedNamespace(parameters.subpath))
                    );
                }
                NamespaceVersionBarrier barrier = new NamespaceVersionBarrier(context, metadata);
                barrier.await(metadata.version(), 20, Duration.ofMillis(250)); // 5000 milliseconds
            }
            // Done. Remove the namespace.
            NamespaceUtil.remove(context, parameters.subpath);
        }, response::writeOK);
    }

    private class PurgeParameters {
        private final List<String> subpath;

        private PurgeParameters(Request request) {
            if (request.getParams().size() != 2) {
                throw wrongNumberOfArguments(request, NamespaceSubcommand.PURGE);
            }
            subpath = readSubpath(request.getParams().get(1));
            validateSubpath(subpath);
        }
    }
}
