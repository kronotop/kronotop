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
import com.kronotop.foundationdb.zmap.protocol.ZIncD128Message;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(ZIncD128Message.COMMAND)
@MaximumParameterCount(ZIncD128Message.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ZIncD128Message.MINIMUM_PARAMETER_COUNT)
public class ZIncD128Handler extends BaseFoundationDBHandler implements Handler {

    public ZIncD128Handler(FoundationDBService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZINCD128).set(new ZIncD128Message(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        runAsync(context, response, () -> {
            ZIncD128Message message = request.attr(MessageTypes.ZINCD128).get();

            Session session = request.getSession();
            Transaction tr = TransactionUtils.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);
            byte[] key = subspace.pack(message.getKey());
            byte[] raw = tr.get(key).join();

            Decimal128 current;
            if (raw == null) {
                current = Decimal128.POSITIVE_ZERO;
            } else {
                if (raw.length != 16) {
                    throw new KronotopException(
                            "Invalid stored value: expected 16-byte Decimal128 (IEEE-754 BID)"
                    );
                }

                ByteBuffer r = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
                long low  = r.getLong();
                long high = r.getLong();
                current = Decimal128.fromIEEE754BIDEncoding(high, low);
            }

            BigDecimal increment;
            try {
                increment = new BigDecimal(message.getValue());
            } catch (NumberFormatException e) {
                throw new KronotopException("ERR invalid decimal", e);
            }

            BigDecimal nextValue = current.bigDecimalValue().add(increment);

            final Decimal128 next;
            try {
                next = new Decimal128(nextValue);
            } catch (NumberFormatException e) {
                throw new KronotopException("ERR decimal overflow", e);
            }

            ByteBuffer w = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            w.putLong(next.getLow());
            w.putLong(next.getHigh());

            tr.set(key, w.array());
            TransactionUtils.commitIfAutoCommitEnabled(tr, session);
        }, response::writeOK);
    }
}
