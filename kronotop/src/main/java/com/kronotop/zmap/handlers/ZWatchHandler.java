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

package com.kronotop.zmap.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.KronotopException;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.zmap.BaseZMapHandler;
import com.kronotop.zmap.ZMapService;
import com.kronotop.zmap.ZWatcher;
import com.kronotop.zmap.handlers.protocol.ZWatchMessage;
import io.netty.channel.Channel;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(ZWatchMessage.COMMAND)
@MinimumParameterCount(ZWatchMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZWatchMessage.MAXIMUM_PARAMETER_COUNT)
public class ZWatchHandler extends BaseZMapHandler implements Handler {
    public ZWatchHandler(ZMapService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZWATCH).set(new ZWatchMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        Session session = request.getSession();
        // Registering requires committing the watch's own snapshot, so ZWATCH cannot participate in an
        // open user transaction. Reject it inside BEGIN.
        if (Boolean.TRUE.equals(session.attr(SessionAttributes.BEGIN).get())) {
            throw new KronotopException("ZWATCH is not allowed within a transaction");
        }

        ZWatcher zwatcher = service.getZWatcher();
        Channel channel = response.getCtx().channel();
        long clientId = session.getClientId();
        Set<byte[]> watchedKeys = session.attr(SessionAttributes.ZWATCH_KEYS).get();

        runAsync(context, response, () -> {
            ZWatchMessage message = request.attr(MessageTypes.ZWATCH).get();

            byte[] packedKey;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                DirectorySubspace subspace = openZMapSubspace(tr, session);
                packedKey = subspace.pack(message.getKey());
            }

            // Record the watched key before registering so a disconnect can release this client's demand
            // on it. A connection can hold several watched keys at once (pipelined ZWATCH), so this is a
            // set rather than a single slot; the entry is removed on every completion path below.
            watchedKeys.add(packedKey);
            try {
                CompletableFuture<Void> signal = zwatcher.register(packedKey, clientId);

                // Close the window where the connection drops while the watch is being registered.
                if (!channel.isActive()) {
                    zwatcher.leave(packedKey, clientId);
                }

                signal.join();
            } finally {
                watchedKeys.remove(packedKey);
            }
        }, response::writeOK);
    }
}
