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
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.namespace.NoSuchNamespaceException;
import com.kronotop.namespace.handlers.protocol.NamespaceSubcommand;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

class ListSubcommand extends BaseSubcommand implements SubcommandHandler {

    ListSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        ListParameters parameters = new ListParameters(request);
        AsyncCommandExecutor.supplyAsync(context, response, () -> {
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                List<String> subpath = getNamespaceSubpath(parameters.subpath);
                CompletableFuture<List<String>> future;
                if (subpath.isEmpty()) {
                    future = context.getDirectoryLayer().list(tr);
                } else {
                    future = context.getDirectoryLayer().list(tr, subpath);
                }
                List<String> result = future.join();
                List<RedisMessage> children = new ArrayList<>();
                for (String namespace : result) {
                    if (namespace.equals(Namespace.INTERNAL_LEAF)) {
                        continue;
                    }
                    children.add(
                            new FullBulkStringRedisMessage(
                                    Unpooled.wrappedBuffer(namespace.getBytes(StandardCharsets.UTF_8)
                                    )
                            )
                    );
                }
                return children;
            } catch (CompletionException e) {
                if (e.getCause() instanceof NoSuchDirectoryException) {
                    if (parameters.subpath.isEmpty()) {
                        // No namespaces directory, the cluster has not been initialized yet
                        return new ArrayList<>();
                    }
                    throw new NoSuchNamespaceException(String.join(".", parameters.subpath));
                }
                throw new KronotopException(e.getCause());
            }
        }, response::writeArray);
    }

    private class ListParameters {
        private final List<String> subpath = new ArrayList<>();

        private ListParameters(Request request) {
            if (request.getParams().size() > 2) {
                throw wrongNumberOfArguments(request, NamespaceSubcommand.LIST);
            }
            if (request.getParams().size() == 2) {
                subpath.addAll(readSubpath(request.getParams().get(1)));
            }
            validateSubpath(subpath);
        }
    }
}
