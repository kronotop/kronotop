/*
 * Copyright (c) 2023 Kronotop
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

import com.apple.foundationdb.Transaction;
import com.kronotop.core.Context;
import com.kronotop.core.TransactionUtils;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.List;

class NamespaceExistsSubcommand extends BaseNamespaceSubcommand implements NamespaceSubcommandExecutor {
    NamespaceExistsSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
        NamespaceMessage.ExistsMessage existsMessage = message.getExistsMessage();

        Transaction tr = TransactionUtils.getOrCreateTransaction(context, request.getChannelContext());
        List<String> subpath = getBaseSubpath().addAll(existsMessage.getPath()).asList();
        Boolean exists = directoryLayer.exists(tr, subpath).join();
        if (exists) {
            response.writeInteger(1);
            return;
        }
        response.writeInteger(0);
    }
}
