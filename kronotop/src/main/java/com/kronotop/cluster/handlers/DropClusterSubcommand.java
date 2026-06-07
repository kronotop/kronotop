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
import io.netty.buffer.ByteBuf;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class DropClusterSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    private static final long TOKEN_TTL_MILLIS = 60_000;

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, DropClusterToken> pendingTokens = new HashMap<>();

    DropClusterSubcommand(RoutingService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        DropClusterParameters parameters = new DropClusterParameters(request.getParams());

        if (!parameters.clusterName.equals(context.getClusterName())) {
            throw new KronotopException("cluster name does not match");
        }

        if (parameters.token == null) {
            lock.lock();
            try {
                DropClusterToken existing = pendingTokens.get(parameters.clusterName);
                if (existing != null) {
                    response.writeFullBulkString(bulkString(existing.token()));
                    return;
                }

                String token = UUID.randomUUID().toString();
                pendingTokens.put(parameters.clusterName, new DropClusterToken(token, System.nanoTime()));
                response.writeFullBulkString(bulkString(token));
                return;
            } finally {
                lock.unlock();
            }
        }

        runAsync(context, response, () -> {
            lock.lock();
            try {
                if (!pendingTokens.containsKey(parameters.clusterName)) {
                    throw new KronotopException("no pending drop-cluster token for this cluster");
                }

                DropClusterToken pending = pendingTokens.get(parameters.clusterName);
                if (!pending.token.equals(parameters.token)) {
                    throw new KronotopException("invalid drop-cluster token");
                }

                long elapsedMillis = (System.nanoTime() - pending.createdAtNanos) / 1_000_000;
                if (elapsedMillis > TOKEN_TTL_MILLIS) {
                    throw new KronotopException("drop-cluster token has expired");
                }

                context.getFoundationDB().run(tr -> {
                    List<String> subpath = KronotopDirectory.kronotop().cluster(parameters.clusterName).toList();
                    return context.getDirectoryLayer().removeIfExists(tr, subpath).join();
                });
                pendingTokens.remove(parameters.clusterName);
            } finally {
                lock.unlock();
            }
        }, response::writeOK);
    }

    private static class DropClusterParameters {
        private final String clusterName;
        private final String token;

        DropClusterParameters(ArrayList<ByteBuf> params) {
            if (params.size() < 2) {
                throw new KronotopException("cluster name is required");
            }

            clusterName = ProtocolMessageUtil.readAsString(params.get(1));
            if (params.size() >= 3) {
                token = ProtocolMessageUtil.readAsString(params.get(2));
            } else {
                token = null;
            }
        }
    }

    private record DropClusterToken(String token, long createdAtNanos) {
    }
}
