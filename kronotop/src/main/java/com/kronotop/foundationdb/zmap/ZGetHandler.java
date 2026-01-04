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

package com.kronotop.foundationdb.zmap;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.foundationdb.BaseFoundationDBHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.zmap.protocol.ZGetMessage;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.CompletableFuture;

@Command(ZGetMessage.COMMAND)
@MinimumParameterCount(ZGetMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZGetMessage.MAXIMUM_PARAMETER_COUNT)
public class ZGetHandler extends BaseFoundationDBHandler implements Handler {
    public ZGetHandler(FoundationDBService service) {
        super(service);
    }

    private CompletableFuture<byte[]> get(Transaction tr, byte[] key, boolean isSnapshot) {
        if (isSnapshot) {
            return tr.snapshot().get(key);
        }
        return tr.get(key);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZGET).set(new ZGetMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        AsyncCommandExecutor.supplyAsync(context, response, () -> {
            ZGetMessage message = request.attr(MessageTypes.ZGET).get();
            Session session = request.getSession();

            Transaction tr = TransactionUtils.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);

            CompletableFuture<byte[]> future = get(tr, subspace.pack(message.getKey()), TransactionUtils.isSnapshotRead(session));
            return future.join();
        }, (value) -> {
            if (value == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }
            ByteBuf buf = Unpooled.wrappedBuffer(value);
            FullBulkStringRedisMessage result = new FullBulkStringRedisMessage(buf);
            response.writeFullBulkString(result);
        });
    }
}