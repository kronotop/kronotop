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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.foundationdb.BaseFoundationDBHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.zmap.protocol.ZIncI64Message;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(ZIncI64Message.COMMAND)
@MaximumParameterCount(ZIncI64Message.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ZIncI64Message.MINIMUM_PARAMETER_COUNT)
public class ZIncI64Handler extends BaseFoundationDBHandler implements Handler {
    public ZIncI64Handler(FoundationDBService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZINCI64).set(new ZIncI64Message(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        runAsync(context, response, () -> {
            ZIncI64Message message = request.attr(MessageTypes.ZINCI64).get();
            Session session = request.getSession();
            Transaction tr = TransactionUtils.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);
            byte[] key = subspace.pack(message.getKey());
            byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(message.getValue()).array();
            tr.mutate(MutationType.ADD, key, value);
            TransactionUtils.commitIfAutoCommitEnabled(tr, session);
        }, response::writeOK);
    }
}
