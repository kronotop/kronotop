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
import com.kronotop.KronotopException;
import com.kronotop.foundationdb.BaseFoundationDBHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.zmap.protocol.ZIncF64Message;
import com.kronotop.foundationdb.zmap.protocol.ZIncI64Message;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(ZIncF64Message.COMMAND)
@MaximumParameterCount(ZIncF64Message.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ZIncF64Message.MINIMUM_PARAMETER_COUNT)
public class ZIncF64Handler extends BaseFoundationDBHandler implements Handler {
    public ZIncF64Handler(FoundationDBService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZINCF64).set(new ZIncF64Message(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        runAsync(context, response, () -> {
            ZIncF64Message message = request.attr(MessageTypes.ZINCF64).get();
            double delta = message.getValue();
            if (!Double.isFinite(delta)) {
                throw new KronotopException("Invalid delta: value must be a finite IEEE-754 double");
            }
            Session session = request.getSession();
            Transaction tr = TransactionUtils.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);
            byte[] key = subspace.pack(message.getKey());
            byte[] raw = tr.get(key).join();
            if (raw != null && raw.length != 8) {
                throw new KronotopException("Invalid stored value: expected 8-byte IEEE-754 double");
            }

            double value = 0.0;
            if (raw != null) {
                 value = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).getDouble();
            }
            double next = value + delta;
            if (!Double.isFinite(next)) {
                throw new KronotopException("Resulting value is not a finite IEEE-754 double (overflow or invalid operation)");
            }

            // normalize
            if (next == -0.0d) {
                next = 0.0d;
            }
            byte[] result = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(next).array();
            tr.set(key, result);
            TransactionUtils.commitIfAutoCommitEnabled(tr, session);
        }, response::writeOK);
    }
}