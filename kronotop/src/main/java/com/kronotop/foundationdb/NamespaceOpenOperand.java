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

import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.foundationdb.protocol.NamespaceMessage;
import com.kronotop.server.Response;

import java.util.List;

public class NamespaceOpenOperand extends BaseNamespaceOperand implements CommandOperand {
    NamespaceOpenOperand(FoundationDBService service, NamespaceMessage namespaceMessage, Response response) {
        super(service, namespaceMessage, response);
    }

    @Override
    public void execute() {
        NamespaceMessage.CreateMessage createMessage = namespaceMessage.getCreateMessage();
        List<String> subpath = getBaseSubpath().addAll(createMessage.getSubpath()).asList();

        if (createMessage.hasLayer()) {
            future = directoryLayer.open(transaction, subpath, createMessage.getLayer().getBytes());
        } else {
            future = directoryLayer.open(transaction, subpath);
        }

        future.handle((directorySubspace, ex) -> {
            if (ex != null) {
                if (ex.getCause() instanceof NoSuchDirectoryException) {
                    String message = NamespaceUtil.NoSuchNamespaceMessage(service.getContext(), ex.getCause());
                    response.writeError(RESPError.NOSUCHNAMESPACE, message);
                    return null;
                } else {
                    throw new KronotopException(ex);
                }
            } else {
                NamespaceCache.add(response, createMessage.getSubpath(), directorySubspace);
                response.writeOK();
                return null;
            }
        }).join();
    }
}
