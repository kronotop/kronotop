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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.zmap.BaseZMapHandler;
import com.kronotop.zmap.ZMapService;
import com.kronotop.zmap.handlers.protocol.ZMutateMessage;

import java.util.Collections;
import java.util.List;

@Command(ZMutateMessage.COMMAND)
@MinimumParameterCount(ZMutateMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZMutateMessage.MAXIMUM_PARAMETER_COUNT)
public class ZMutateHandler extends BaseZMapHandler implements Handler {
    public ZMutateHandler(ZMapService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(new String(request.attr(MessageTypes.ZMUTATE).get().getKey()));
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZMUTATE).set(new ZMutateMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        AsyncCommandExecutor.runAsync(context, response, () -> {
            ZMutateMessage message = request.attr(MessageTypes.ZMUTATE).get();

            Session session = request.getSession();
            Transaction tr = TransactionUtil.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session);

            tr.mutate(message.getMutationType(), subspace.pack(message.getKey()), message.getParam());
            TransactionUtil.commitIfAutoCommitEnabled(tr, session);
        }, response::writeOK);
    }
}
