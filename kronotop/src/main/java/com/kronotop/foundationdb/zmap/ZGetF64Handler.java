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
import com.kronotop.foundationdb.zmap.protocol.ZGetF64Message;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(ZGetF64Message.COMMAND)
@MaximumParameterCount(ZGetF64Message.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ZGetF64Message.MINIMUM_PARAMETER_COUNT)
public class ZGetF64Handler extends BaseFoundationDBHandler implements Handler {
    public ZGetF64Handler(FoundationDBService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZGETF64).set(new ZGetF64Message(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            ZGetF64Message message = request.attr(MessageTypes.ZGETF64).get();
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
            if (value.length != 8) {
                throw new KronotopException("Invalid stored value: expected 8-byte IEEE-754 double");
            }
            double result = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getDouble();
            response.writeDouble(result);
        });
    }
}