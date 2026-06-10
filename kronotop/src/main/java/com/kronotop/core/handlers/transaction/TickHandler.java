/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.core.handlers.transaction;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.core.handlers.transaction.protocol.TickMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.concurrent.atomic.AtomicReference;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(TickMessage.COMMAND)
@MaximumParameterCount(TickMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(TickMessage.MINIMUM_PARAMETER_COUNT)
public class TickHandler implements Handler {
    private static final long CACHE_TTL_MS = 1000;
    private final Context context;
    private final AtomicReference<CachedReadVersion> cachedReadVersion = new AtomicReference<>();

    public TickHandler(Context context) {
        this.context = context;
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.TICK).set(new TickMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            TickMessage message = request.attr(MessageTypes.TICK).get();
            if (message.getMode().equals(TickMessage.Mode.FRESH)) {
                return fetchReadVersion();
            }
            return cachedTick();
        }, response::writeInteger);
    }

    private long cachedTick() {
        CachedReadVersion current = cachedReadVersion.get();
        if (current != null && System.currentTimeMillis() - current.timestamp() < CACHE_TTL_MS) {
            return current.readVersion();
        }
        long fresh = fetchReadVersion();
        CachedReadVersion candidate = new CachedReadVersion(System.currentTimeMillis(), fresh);
        // The cached read version must never go backwards; keep the entry with the max version.
        CachedReadVersion winner = cachedReadVersion.accumulateAndGet(candidate,
                (cur, cand) ->
                        cur == null || cand.readVersion() > cur.readVersion() ? cand : cur
        );
        return winner.readVersion();
    }

    private long fetchReadVersion() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getReadVersion().join();
        }
    }

    private record CachedReadVersion(long timestamp, long readVersion) {
    }
}
