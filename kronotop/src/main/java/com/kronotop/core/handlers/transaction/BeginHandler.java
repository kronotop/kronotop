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

package com.kronotop.core.handlers.transaction;

import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.core.handlers.transaction.protocol.BeginMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.transaction.TransactionUtil;
import io.netty.util.Attribute;

@Command(BeginMessage.COMMAND)
@MaximumParameterCount(BeginMessage.MAXIMUM_PARAMETER_COUNT)
public class BeginHandler implements Handler {
    private final Context context;

    public BeginHandler(Context context) {
        this.context = context;
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BEGIN).set(new BeginMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        AsyncCommandExecutor.runAsync(context, response, () -> {
            Session session = request.getSession();
            Attribute<Boolean> beginAttr = session.attr(SessionAttributes.BEGIN);
            if (Boolean.TRUE.equals(beginAttr.get())) {
                throw new KronotopException(RESPError.TRANSACTION, "there is already a transaction in progress.");
            }
            session.setTransaction(TransactionUtil.createInstrumentedTransaction(context));
        }, response::writeOK);
    }
}
