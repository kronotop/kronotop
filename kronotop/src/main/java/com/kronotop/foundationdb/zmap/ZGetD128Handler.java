/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.foundationdb.zmap;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.KronotopException;
import com.kronotop.foundationdb.BaseFoundationDBHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.zmap.protocol.ZGetD128Message;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.Unpooled;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(ZGetD128Message.COMMAND)
@MaximumParameterCount(ZGetD128Message.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ZGetD128Message.MINIMUM_PARAMETER_COUNT)
public class ZGetD128Handler extends BaseFoundationDBHandler implements Handler {
    public ZGetD128Handler(FoundationDBService service) {
        super(service);
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
            Transaction tr = TransactionUtils.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);
            byte[] key = subspace.pack(message.getKey());
            return tr.get(key).join();
        }, (value) -> {
            if (value == null) {
                response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                return;
            }
            if (value.length != 16) {
                throw new KronotopException(
                        "Invalid stored value: expected 16-byte Decimal128 (IEEE-754 BID)"
                );
            }

            ByteBuffer r = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
            long low = r.getLong();
            long high = r.getLong();
            Decimal128 current = Decimal128.fromIEEE754BIDEncoding(high, low);

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