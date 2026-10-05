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

package com.kronotop.cluster.handlers;

import com.kronotop.KronotopException;
import com.kronotop.cluster.RoutingService;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.ByteBuf;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class DropClusterSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    static final long TOKEN_TTL_MILLIS = 60_000;
    final Map<String, DropClusterToken> pendingTokens = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    DropClusterSubcommand(RoutingService service) {
        super(service);
    }

    private boolean isTokenExpired(DropClusterToken token) {
        long elapsedMillis = (System.nanoTime() - token.createdAtNanos) / 1_000_000;
        return elapsedMillis > TOKEN_TTL_MILLIS;
    }

    @Override
    public void execute(Request request, Response response) {
        DropClusterArguments arguments = new DropClusterArguments(request.getArguments());

        if (!arguments.clusterName.equals(context.getClusterName())) {
            throw new KronotopException("cluster name does not match");
        }

        if (arguments.token == null) {
            lock.lock();
            try {
                DropClusterToken existing = pendingTokens.get(arguments.clusterName);
                if (existing != null && !isTokenExpired(existing)) {
                    response.writeFullBulkString(bulkString(existing.token()));
                    return;
                }

                String token = UUID.randomUUID().toString();
                pendingTokens.put(arguments.clusterName, new DropClusterToken(token, System.nanoTime()));
                response.writeFullBulkString(bulkString(token));
                return;
            } finally {
                lock.unlock();
            }
        }

        runAsync(context, response, () -> {
            lock.lock();
            try {
                if (!pendingTokens.containsKey(arguments.clusterName)) {
                    throw new KronotopException("no pending drop-cluster token for this cluster");
                }

                DropClusterToken pending = pendingTokens.get(arguments.clusterName);
                if (!pending.token().equals(arguments.token)) {
                    throw new KronotopException("invalid drop-cluster token");
                }

                if (isTokenExpired(pending)) {
                    pendingTokens.remove(arguments.clusterName);
                    throw new KronotopException("drop-cluster token has expired");
                }

                TransactionUtil.executeThenCommit(context, tr -> {
                    List<String> subpath = KronotopDirectory.kronotop().cluster(arguments.clusterName).toList();
                    return context.getDirectoryLayer().removeIfExists(tr, subpath).join();
                });
                pendingTokens.remove(arguments.clusterName);
            } finally {
                lock.unlock();
            }
        }, response::writeOK);
    }

    private static class DropClusterArguments {
        private final String clusterName;
        private final String token;

        DropClusterArguments(ArrayList<ByteBuf> args) {
            if (args.size() < 2) {
                throw new KronotopException("cluster name is required");
            }
            if (args.size() > 3) {
                throw new InvalidNumberOfArgumentsException();
            }

            clusterName = ProtocolMessageUtil.readAsString(args.get(1));
            if (args.size() == 3) {
                token = ProtocolMessageUtil.readAsString(args.get(2));
            } else {
                token = null;
            }
        }
    }

    record DropClusterToken(String token, long createdAtNanos) {
    }
}
