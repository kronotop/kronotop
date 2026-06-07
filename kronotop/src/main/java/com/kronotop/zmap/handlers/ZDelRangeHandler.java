/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.zmap.handlers;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.zmap.BaseZMapHandler;
import com.kronotop.zmap.ZMapService;
import com.kronotop.zmap.handlers.protocol.ZDelRangeMessage;

import java.util.Arrays;

@Command(ZDelRangeMessage.COMMAND)
@MinimumParameterCount(ZDelRangeMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZDelRangeMessage.MAXIMUM_PARAMETER_COUNT)
public class ZDelRangeHandler extends BaseZMapHandler implements Handler {
    public ZDelRangeHandler(ZMapService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZDELRANGE).set(new ZDelRangeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        AsyncCommandExecutor.runAsync(context, response, () -> {
            ZDelRangeMessage message = request.attr(MessageTypes.ZDELRANGE).get();

            Session session = request.getSession();
            Transaction tr = TransactionUtil.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);

            byte[] begin;
            byte[] end;
            if (Arrays.equals(message.getBegin(), ASTERISK)) {
                begin = subspace.pack();
            } else {
                begin = subspace.pack(message.getBegin());
            }

            if (Arrays.equals(message.getEnd(), ASTERISK)) {
                end = ByteArrayUtil.strinc(subspace.pack());
            } else {
                end = subspace.pack(message.getEnd());
            }

            Range range = new Range(begin, end);
            tr.clear(range);
            TransactionUtil.commitIfAutoCommitEnabled(tr, session);
        }, response::writeOK);
    }
}