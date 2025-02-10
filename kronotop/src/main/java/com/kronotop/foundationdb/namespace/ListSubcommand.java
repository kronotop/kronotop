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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.Context;
import com.kronotop.common.KronotopException;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

class ListSubcommand extends BaseSubcommand implements SubcommandExecutor {

    ListSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
        NamespaceMessage.ListMessage listMessage = message.getListMessage();

        Transaction tr = context.getFoundationDB().createTransaction();
        List<String> subpath = getNamespaceSubpath(listMessage.getSubpath());
        CompletableFuture<List<String>> future;
        if (subpath.isEmpty()) {
            future = directoryLayer.list(tr);
        } else {
            future = directoryLayer.list(tr, subpath);
        }

        try {
            List<String> result = future.join();
            List<RedisMessage> children = new ArrayList<>();
            for (String namespace : result) {
                children.add(new SimpleStringRedisMessage(namespace));
            }
            response.writeArray(children);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                if (listMessage.getSubpath().isEmpty()) {
                    // No namespaces directory, the cluster has not been initialized yet
                    response.writeArray(List.of());
                    return;
                }
                throw new NoSuchNamespaceException(String.join(".", listMessage.getSubpath()));
            }
            throw new KronotopException(e.getCause());
        }
    }
}
