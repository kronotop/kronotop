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

import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.namespace.handlers.protocol.NamespaceMessage;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

class ExistsSubcommand extends BaseSubcommand implements SubcommandExecutor {
    ExistsSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        AsyncCommandExecutor.supplyAsync(context, response, () -> {
            NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
            NamespaceMessage.ExistsMessage existsMessage = message.getExistsMessage();
            return NamespaceUtil.exists(context, existsMessage.getSubpath());
        }, (exists) -> {
            if (exists) {
                response.writeInteger(1);
                return;
            }
            response.writeInteger(0);
        });
    }
}
