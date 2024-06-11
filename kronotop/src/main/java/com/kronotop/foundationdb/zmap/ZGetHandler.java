/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.kronotop.NamespaceUtils;
import com.kronotop.TransactionUtils;
import com.kronotop.foundationdb.BaseHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.foundationdb.zmap.protocol.ZGetMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.CompletableFuture;

@Command(ZGetMessage.COMMAND)
@MinimumParameterCount(ZGetMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZGetMessage.MAXIMUM_PARAMETER_COUNT)
public class ZGetHandler extends BaseHandler implements Handler {
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
        ZGetMessage zGetMessage = request.attr(MessageTypes.ZGET).get();

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getChannelContext());
        Namespace namespace = NamespaceUtils.open(service.getContext(), request.getChannelContext(), tr);

        CompletableFuture<byte[]> future = get(tr, namespace.getZMap().pack(zGetMessage.getKey()), TransactionUtils.isSnapshotRead(request.getChannelContext()));
        byte[] result = future.join();
        if (result == null) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        ByteBuf buf = response.getChannelContext().alloc().buffer();
        try {
            buf.writeBytes(result);
            response.write(buf);
        } catch (Exception e) {
            if (buf.refCnt() > 0) {
                ReferenceCountUtil.release(buf);
            }
            throw e;
        }
    }
}