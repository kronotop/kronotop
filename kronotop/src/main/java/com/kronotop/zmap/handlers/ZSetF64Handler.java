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
import com.kronotop.zmap.handlers.protocol.ZSetF64Message;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(ZSetF64Message.COMMAND)
@MaximumParameterCount(ZSetF64Message.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ZSetF64Message.MINIMUM_PARAMETER_COUNT)
public class ZSetF64Handler extends BaseZMapHandler implements Handler {
    public ZSetF64Handler(ZMapService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZSETF64).set(new ZSetF64Message(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        runAsync(context, response, () -> {
            ZSetF64Message message = request.attr(MessageTypes.ZSETF64).get();
            ZMapNumericValueCodec.validateFiniteF64(message.getValue(), "Invalid value: must be a finite IEEE-754 double");
            Session session = request.getSession();
            Transaction tr = TransactionUtil.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);
            byte[] key = subspace.pack(message.getKey());
            tr.set(key, ZMapNumericValueCodec.encodeF64(message.getValue()));
            TransactionUtil.commitIfAutoCommitEnabled(tr, session);
        }, response::writeOK);
    }
}
