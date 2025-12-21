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

package com.kronotop.namespace.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.namespace.handlers.protocol.NamespaceMessage;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class CreateSubcommand extends BaseSubcommand implements SubcommandExecutor {

    CreateSubcommand(Context context) {
        super(context);
    }

    public void execute(Request request, Response response) {
        runAsync(context, response, () -> {
            NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
            // Create the namespace by using an isolated, one-off transaction to prevent nasty consistency bugs.
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                NamespaceUtil.create(context, tr, message.getCreateMessage().getSubpath());
            }
        }, response::writeOK);
    }
}
