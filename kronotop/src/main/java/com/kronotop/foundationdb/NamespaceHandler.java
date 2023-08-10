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
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MinimumParameterCount;

@Command(NamespaceMessage.COMMAND)
@MinimumParameterCount(NamespaceMessage.MINIMUM_PARAMETER_COUNT)
public class NamespaceHandler implements Handler {
    private final FoundationDBService service;

    NamespaceHandler(FoundationDBService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.NAMESPACE).set(new NamespaceMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        NamespaceMessage namespaceMessage = request.attr(MessageTypes.NAMESPACE).get();
        String operand = namespaceMessage.getOperand().toUpperCase();

        CommandOperand cp;
        switch (operand) {
            case NamespaceMessage.CREATE_OPERAND:
                cp = new NamespaceCreateOperand(service, namespaceMessage, response);
                break;
            case NamespaceMessage.LIST_OPERAND:
                cp = new NamespaceListOperand(service, namespaceMessage, response);
                break;
            case NamespaceMessage.CREATE_OR_OPEN_OPERAND:
                cp = new NamespaceCreateOrOpenOperand(service, namespaceMessage, response);
                break;
            case NamespaceMessage.OPEN_OPERAND:
                cp = new NamespaceOpenOperand(service, namespaceMessage, response);
                break;
            case NamespaceMessage.LIST_OPEN_OPERAND:
                cp = new NamespaceListOpenOperand(service, namespaceMessage, response);
                break;
            case NamespaceMessage.MOVE_OPERAND:
                cp = new NamespaceMoveOperand(service, namespaceMessage, response);
                break;
            case NamespaceMessage.REMOVE_OPERAND:
                cp = new NamespaceRemoveOperand(service, namespaceMessage, response);
                break;
            case NamespaceMessage.EXISTS_OPERAND:
                cp = new NamespaceExistsOperand(service, namespaceMessage, response);
                break;
            default:
                throw new UnknownOperandException(String.format("unknown operand: %s", namespaceMessage.getOperand()));
        }
        cp.execute();
    }
}
