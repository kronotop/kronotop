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
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.foundationdb.protocol.NamespaceMessage;
import com.kronotop.server.Response;

import java.util.List;


class NamespaceCreateOperand extends BaseNamespaceOperand implements CommandOperand {
    NamespaceCreateOperand(FoundationDBService service, NamespaceMessage namespaceMessage, Response response) {
        super(service, namespaceMessage, response);
    }

    public void execute() {
        NamespaceMessage.CreateMessage createMessage = namespaceMessage.getCreateMessage();

        List<String> subpath = getBaseSubpath().addAll(createMessage.getSubpath()).asList();
        if (createMessage.hasLayer() && createMessage.hasPrefix()) {
            future = directoryLayer.create(
                    transaction,
                    subpath,
                    createMessage.getLayer().getBytes(),
                    createMessage.getPrefix().getBytes()
            );
        } else if (createMessage.hasLayer()) {
            future = directoryLayer.create(
                    transaction,
                    subpath,
                    createMessage.getLayer().getBytes()
            );
        } else {
            future = directoryLayer.create(
                    transaction,
                    subpath
            );
        }

        future.handle((result, throwable) -> {
            if (throwable != null) {
                if (throwable.getCause() instanceof DirectoryAlreadyExistsException) {
                    String message = NamespaceUtil.NamespaceAlreadyExistsMessage(service.getContext(), throwable.getCause());
                    response.writeError(RESPError.NAMESPACEALREADYEXISTS, message);
                    return null;
                } else {
                    throw new KronotopException(throwable);
                }
            }
            transaction.commit().join();
            response.writeOK();
            return null;
        }).join();
    }
}
