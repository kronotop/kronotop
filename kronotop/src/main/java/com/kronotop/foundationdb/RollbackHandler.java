/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.foundationdb;

import com.apple.foundationdb.Transaction;
import com.kronotop.foundationdb.protocol.RollbackMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import io.netty.util.Attribute;

@Command(RollbackMessage.COMMAND)
@MaximumParameterCount(RollbackMessage.MAXIMUM_PARAMETER_COUNT)
class RollbackHandler extends BaseFoundationDBHandler implements Handler {

    RollbackHandler(FoundationDBService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ROLLBACK).set(new RollbackMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        Session session = request.getSession();
        Attribute<Boolean> beginAttr = session.attr(SessionAttributes.BEGIN);
        if (!Boolean.TRUE.equals(beginAttr.get())) {
            response.writeError(RESPError.TRANSACTION, "there is no transaction in progress.");
            return;
        }

        Attribute<Transaction> transactionAttr = session.attr(SessionAttributes.TRANSACTION);
        Transaction tr = transactionAttr.get();
        try {
            tr.cancel();
        } finally {
            session.unsetTransaction();
        }

        response.writeOK();
    }
}