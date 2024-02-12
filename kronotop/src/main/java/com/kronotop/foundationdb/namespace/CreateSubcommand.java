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
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.common.KronotopException;
import com.kronotop.core.Context;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


class CreateSubcommand extends BaseSubcommand implements SubcommandExecutor {

    CreateSubcommand(Context context) {
        super(context);
    }

    public void execute(Request request, Response response) {
        NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();

        // Create the namespace by using an isolated, one-off transaction to prevent weird consistency bugs.
        Transaction tr = context.getFoundationDB().createTransaction();
        CompletableFuture<DirectorySubspace> result;
        List<String> subpath = getBaseSubpath().addAll(message.getCreateMessage().getSubpath()).asList();
        if (message.getCreateMessage().hasLayer() && message.getCreateMessage().hasPrefix()) {
            result = directoryLayer.create(
                    tr,
                    subpath,
                    message.getCreateMessage().getLayer().getBytes(),
                    message.getCreateMessage().getPrefix().getBytes()
            );
        } else if (message.getCreateMessage().hasLayer()) {
            result = directoryLayer.create(
                    tr,
                    subpath,
                    message.getCreateMessage().getLayer().getBytes()
            );
        } else {
            result = directoryLayer.create(
                    tr,
                    subpath
            );
        }

        try {
            result.join();
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new NamespaceAlreadyExistsException(dottedNamespace(message.getCreateMessage().getSubpath()));
            }
            throw new KronotopException(e.getCause());
        }

        response.writeOK();
    }
}
