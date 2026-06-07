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
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.zmap.BaseZMapHandler;
import com.kronotop.zmap.ZMapNumericValueCodec;
import com.kronotop.zmap.ZMapService;
import com.kronotop.zmap.handlers.protocol.ZIncD128Message;
import org.bson.types.Decimal128;

import java.math.BigDecimal;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(ZIncD128Message.COMMAND)
@MaximumParameterCount(ZIncD128Message.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ZIncD128Message.MINIMUM_PARAMETER_COUNT)
public class ZIncD128Handler extends BaseZMapHandler implements Handler {

    public ZIncD128Handler(ZMapService service) {
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
            Transaction tr = TransactionUtil.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);
            byte[] key = subspace.pack(message.getKey());
            byte[] raw = tr.get(key).join();

            Decimal128 current;
            if (raw == null) {
                current = Decimal128.POSITIVE_ZERO;
            } else {
                current = ZMapNumericValueCodec.decodeD128(raw);
            }

            Decimal128 increment = ZMapNumericValueCodec.parseDecimal128(message.getValue());
            BigDecimal nextValue = current.bigDecimalValue().add(increment.bigDecimalValue());

            final Decimal128 next;
            try {
                next = new Decimal128(nextValue);
            } catch (NumberFormatException e) {
                throw new KronotopException("ERR decimal overflow", e);
            }
            tr.set(key, ZMapNumericValueCodec.encodeD128(next));
            TransactionUtil.commitIfAutoCommitEnabled(tr, session);
        }, response::writeOK);
    }
}
