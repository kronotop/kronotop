/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.common.KronotopException;
import com.kronotop.Context;
import com.kronotop.TransactionUtils;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.List;
import java.util.concurrent.CompletionException;

class MoveSubcommand extends BaseSubcommand implements SubcommandExecutor {

    MoveSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
        NamespaceMessage.MoveMessage moveMessage = message.getMoveMessage();

        List<String> oldPath = getBaseSubpath().addAll(moveMessage.getOldPath()).asList();
        List<String> newPath = getBaseSubpath().addAll(moveMessage.getNewPath()).asList();

        Transaction tr = TransactionUtils.getOrCreateTransaction(context, request.getChannelContext());
        try {
            directoryLayer.move(tr, oldPath, newPath).join();
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(String.join(".", moveMessage.getOldPath()));
            } else if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new NamespaceAlreadyExistsException(String.join(".", moveMessage.getNewPath()));
            }
            throw new KronotopException(e.getCause());
        }
        response.writeOK();
    }
}
