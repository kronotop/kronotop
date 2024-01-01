/*
 * Copyright (c) 2023 Kronotop
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
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.foundationdb.zmap.protocol.RangeKeySelector;
import com.kronotop.foundationdb.zmap.protocol.ZGetKeyMessage;
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
import java.util.concurrent.ExecutionException;

@Command(ZGetKeyMessage.COMMAND)
@MinimumParameterCount(ZGetKeyMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZGetKeyMessage.MAXIMUM_PARAMETER_COUNT)
class ZGetKeyHandler extends BaseZMapHandler implements Handler {
    public ZGetKeyHandler(ZMapService service) {
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
        ZGetKeyMessage zGetKeyMessage = request.attr(MessageTypes.ZGETKEY).get();
        Subspace subspace = getSubspace(response, zGetKeyMessage.getNamespace());
        KeySelector keySelector = RangeKeySelector.getKeySelector(
                zGetKeyMessage.getKeySelector(),
                subspace.pack(zGetKeyMessage.getKey())
        );

        Transaction tr = getOrCreateTransaction(response);
        CompletableFuture<byte[]> future = getKey(tr, keySelector, isSnapshotRead(response));
        byte[] result = future.get();
        if (result == null) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        ByteBuf buf = response.getContext().alloc().buffer();
        try {
            buf.writeBytes(subspace.unpack(result).getBytes(0));
            response.write(buf);
        } catch (Exception e) {
            if (buf.refCnt() > 0) {
                ReferenceCountUtil.release(buf);
            }
            throw e;
        }
    }
}