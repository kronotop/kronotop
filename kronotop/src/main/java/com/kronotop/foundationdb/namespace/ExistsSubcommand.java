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

package com.kronotop.foundationdb.namespace;

import com.kronotop.Context;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.internal.NamespaceUtils;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.concurrent.CompletableFuture;

class ExistsSubcommand extends BaseSubcommand implements SubcommandExecutor {
    ExistsSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        CompletableFuture.supplyAsync(() -> {
            NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
            NamespaceMessage.ExistsMessage existsMessage = message.getExistsMessage();
            return NamespaceUtils.exists(context, existsMessage.getPath());
        }, context.getVirtualThreadPerTaskExecutor()).thenAcceptAsync((exists) -> {
            if (exists) {
                response.writeInteger(1);
                return;
            }
            response.writeInteger(0);
        }, response.getCtx().executor()).exceptionally((ex) -> {
            response.writeError(ex);
            return null;
        });
    }
}
