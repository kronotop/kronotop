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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.Transaction;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.foundationdb.BaseFoundationDBHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.foundationdb.zmap.protocol.RangeKeySelector;
import com.kronotop.foundationdb.zmap.protocol.ZGetKeyMessage;
import com.kronotop.internal.NamespaceUtils;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Command(ZGetKeyMessage.COMMAND)
@MinimumParameterCount(ZGetKeyMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZGetKeyMessage.MAXIMUM_PARAMETER_COUNT)
public class ZGetKeyHandler extends BaseFoundationDBHandler implements Handler {
    public ZGetKeyHandler(FoundationDBService service) {
        super(service);
    }

    private CompletableFuture<byte[]> getKey(Transaction tr, KeySelector selector, boolean isSnapshot) {
        if (isSnapshot) {
            return tr.snapshot().getKey(selector);
        }
        return tr.getKey(selector);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZGETKEY).set(new ZGetKeyMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws ExecutionException, InterruptedException {
        AsyncCommandExecutor.supplyAsync(context, response, () -> {
            ZGetKeyMessage message = request.attr(MessageTypes.ZGETKEY).get();

            Session session = request.getSession();
            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getSession());
            Namespace namespace = NamespaceUtils.open(service.getContext(), session, tr);

            KeySelector keySelector = RangeKeySelector.getKeySelector(
                    message.getKeySelector(),
                    namespace.getZMap().pack(message.getKey())
            );

            CompletableFuture<byte[]> future = getKey(tr, keySelector, TransactionUtils.isSnapshotRead(session));
            return new Result(namespace, future.join());
        }, (result) -> {
            if (result.value() == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }

            ByteBuf buf = response.getCtx().alloc().buffer();
            try {
                buf.writeBytes(result.namespace().getZMap().unpack(result.value()).getBytes(0));
                response.write(buf);
            } catch (Exception e) {
                ReferenceCountUtil.release(buf);
                throw e;
            }
        });
    }

    record Result(Namespace namespace, byte[] value) {
    }
}