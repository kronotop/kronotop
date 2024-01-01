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

package com.kronotop.foundationdb;

import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.foundationdb.protocol.NamespaceMessage;
import com.kronotop.server.Response;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class NamespaceMoveOperand extends BaseNamespaceOperand implements CommandOperand {
    NamespaceMoveOperand(FoundationDBService service, NamespaceMessage namespaceMessage, Response response) {
        super(service, namespaceMessage, response);
    }

    @Override
    public void execute() {
        NamespaceMessage.MoveMessage moveMessage = namespaceMessage.getMoveMessage();
        List<String> oldPath = getBaseSubpath().addAll(moveMessage.getOldPath()).asList();
        List<String> newPath = getBaseSubpath().addAll(moveMessage.getNewPath()).asList();

        CompletableFuture<DirectorySubspace> future = directoryLayer.move(transaction, oldPath, newPath);
        future.handle((result, ex) -> {
            if (ex != null) {
                if (ex.getCause() instanceof NoSuchDirectoryException) {
                    String message = NamespaceUtil.NoSuchNamespaceMessage(service.getContext(), ex.getCause());
                    response.writeError(RESPError.NOSUCHNAMESPACE, message);
                } else if (ex.getCause() instanceof DirectoryAlreadyExistsException) {
                    String message = NamespaceUtil.NamespaceAlreadyExistsMessage(service.getContext(), ex.getCause());
                    response.writeError(RESPError.NAMESPACEALREADYEXISTS, message);
                } else {
                    throw new KronotopException(ex);
                }
                return null;
            }
            transaction.commit().join();
            NamespaceCache.replace(response, moveMessage.getOldPath(), moveMessage.getNewPath());
            response.writeOK();
            return null;
        }).join();
    }
}
