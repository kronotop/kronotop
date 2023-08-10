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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.foundationdb.zmap.protocol.ZGetMessage;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.CompletableFuture;

@Command(ZGetMessage.COMMAND)
@MinimumParameterCount(ZGetMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZGetMessage.MAXIMUM_PARAMETER_COUNT)
class ZGetHandler extends BaseZMapHandler implements Handler {
    public ZGetHandler(ZMapService service) {
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

        Subspace subspace = getSubspace(response, zGetMessage.getNamespace());
        Transaction tr = getOrCreateTransaction(response);
        CompletableFuture<byte[]> future = get(tr, subspace.pack(zGetMessage.getKey()), isSnapshotRead(response));
        byte[] result = future.join();
        if (result == null) {
            response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        ByteBuf buf = response.getContext().alloc().buffer();
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