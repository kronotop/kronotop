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

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.*;
import com.kronotop.transaction.InstrumentedTransaction;

import static com.kronotop.AsyncCommandExecutor.runAsync;

/**
 * Cancels an open transaction of this member. The owner session gets a TRANSACTION_CANCELLED error
 * on its next command and cleans up with ROLLBACK. Internal transactions are never cancelled.
 */
class KillSubcommand implements SubcommandHandler {
    private final Context context;

    KillSubcommand(Context context) {
        this.context = context;
    }

    @Override
    public void execute(Request request, Response response) {
        // The argument buffers are released before the task runs. Read them here.
        if (request.getArguments().size() != 2) {
            throw new WrongNumberOfArgumentsException("wrong number of arguments for 'TRANSACTION KILL' command");
        }
        long id = ProtocolMessageUtil.readAsLong(request.getArguments().get(1));
        runAsync(context, response, () -> {
            InstrumentedTransaction tr = context.getTransactionManager().get(id);
            if (tr == null) {
                throw new KronotopException(RESPError.TRANSACTION, "no such transaction: " + id);
            }
            if (tr.getClientId() == InstrumentedTransaction.INTERNAL) {
                throw new KronotopException(RESPError.TRANSACTION, "internal transactions cannot be killed");
            }
            try {
                tr.cancel();
            } catch (IllegalStateException e) {
                // The transaction was closed while we looked at it.
                throw new KronotopException(RESPError.TRANSACTION, "no such transaction: " + id);
            }
        }, response::writeOK);
    }
}
