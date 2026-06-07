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
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.zmap.BaseZMapHandler;
import com.kronotop.zmap.ZMapNumericValueCodec;
import com.kronotop.zmap.ZMapService;
import com.kronotop.zmap.handlers.protocol.ZSetD128Message;
import org.bson.types.Decimal128;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(ZSetD128Message.COMMAND)
@MaximumParameterCount(ZSetD128Message.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ZSetD128Message.MINIMUM_PARAMETER_COUNT)
public class ZSetD128Handler extends BaseZMapHandler implements Handler {
    public ZSetD128Handler(ZMapService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZSETD128).set(new ZSetD128Message(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        runAsync(context, response, () -> {
            ZSetD128Message message = request.attr(MessageTypes.ZSETD128).get();
            Decimal128 parsed = ZMapNumericValueCodec.parseDecimal128(message.getValue());
            Session session = request.getSession();
            Transaction tr = TransactionUtil.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);
            byte[] key = subspace.pack(message.getKey());
            tr.set(key, ZMapNumericValueCodec.encodeD128(parsed));
            TransactionUtil.commitIfAutoCommitEnabled(tr, session);
        }, response::writeOK);
    }
}
