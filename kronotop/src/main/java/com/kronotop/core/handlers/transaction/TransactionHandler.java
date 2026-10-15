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
import com.kronotop.core.handlers.transaction.protocol.TransactionMessage;
import com.kronotop.core.handlers.transaction.protocol.TransactionSubcommand;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumArgumentCount;

import java.util.EnumMap;

@Command(TransactionMessage.COMMAND)
@MinimumArgumentCount(TransactionMessage.MINIMUM_ARGUMENT_COUNT)
public class TransactionHandler implements Handler {
    private final EnumMap<TransactionSubcommand, SubcommandHandler> executors = new EnumMap<>(TransactionSubcommand.class);

    public TransactionHandler(Context context) {
        executors.put(TransactionSubcommand.LIST, new ListSubcommand(context));
        executors.put(TransactionSubcommand.KILL, new KillSubcommand(context));
        executors.put(TransactionSubcommand.SLOW, new SlowSubcommand(context));
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.TRANSACTION).set(new TransactionMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        TransactionMessage message = request.attr(MessageTypes.TRANSACTION).get();

        SubcommandHandler executor = executors.get(message.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
