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

import com.kronotop.foundationdb.protocol.NamespaceMessage;
import com.kronotop.server.Response;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class NamespaceExistsOperand extends BaseNamespaceOperand implements CommandOperand {
    NamespaceExistsOperand(FoundationDBService service, NamespaceMessage namespaceMessage, Response response) {
        super(service, namespaceMessage, response);
    }

    @Override
    public void execute() {
        NamespaceMessage.ExistsMessage existsMessage = namespaceMessage.getExistsMessage();
        List<String> subpath = getBaseSubpath().addAll(existsMessage.getPath()).asList();
        CompletableFuture<Boolean> future = directoryLayer.exists(transaction, subpath);
        Boolean exists = future.join();
        if (exists) {
            response.writeInteger(1);
            return;
        }
        response.writeInteger(0);
    }
}
