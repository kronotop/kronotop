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

import com.kronotop.Context;
import com.kronotop.namespace.NoSuchNamespaceException;
import com.kronotop.namespace.handlers.protocol.NamespaceMessage;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SessionAttributes;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class UseSubcommand extends BaseSubcommand implements SubcommandExecutor {

    UseSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        runAsync(context, response, () -> {
            NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
            NamespaceMessage.UseMessage useMessage = message.getUseMessage();

            String namespace = dottedNamespace(useMessage.getSubpath());
            if (!NamespaceUtil.exists(context, useMessage.getSubpath())) {
                throw new NoSuchNamespaceException(namespace);
            }
            request.getSession().attr(SessionAttributes.CURRENT_NAMESPACE).set(namespace);
        }, response::writeOK);
    }
}

