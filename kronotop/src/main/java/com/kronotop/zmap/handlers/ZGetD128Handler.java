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

package com.kronotop.zmap.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.KronotopException;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.zmap.BaseZMapHandler;
import com.kronotop.zmap.ZMapNumericValueCodec;
import com.kronotop.zmap.ZMapService;
import com.kronotop.zmap.handlers.protocol.ZGetD128Message;
import io.netty.buffer.Unpooled;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(ZGetD128Message.COMMAND)
@MaximumParameterCount(ZGetD128Message.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ZGetD128Message.MINIMUM_PARAMETER_COUNT)
public class ZGetD128Handler extends BaseZMapHandler implements Handler {
    public ZGetD128Handler(ZMapService service) {
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
        request.attr(MessageTypes.ZGETD128).set(new ZGetD128Message(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            ZGetD128Message message = request.attr(MessageTypes.ZGETD128).get();
            Session session = request.getSession();
            Transaction tr = TransactionUtil.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);
            byte[] key = subspace.pack(message.getKey());
            return get(tr, key, TransactionUtil.isSnapshotRead(session)).join();
        }, (value) -> {
            if (value == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }
            Decimal128 current = ZMapNumericValueCodec.decodeD128(value);

            final BigDecimal bd;
            try {
                bd = current.bigDecimalValue();
            } catch (ArithmeticException e) {
                throw new KronotopException(
                        "Invalid stored value: Decimal128 is not representable as BigDecimal",
                        e
                );
            }
            byte[] bytes = bd.toPlainString().getBytes(StandardCharsets.UTF_8);
            FullBulkStringRedisMessage msg =
                    new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(bytes));
            response.writeFullBulkString(msg);
        });
    }
}