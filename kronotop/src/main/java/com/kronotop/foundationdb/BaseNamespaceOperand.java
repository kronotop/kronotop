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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.foundationdb.protocol.NamespaceMessage;
import com.kronotop.server.ChannelAttributes;
import com.kronotop.server.Response;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

import java.util.concurrent.CompletableFuture;

class BaseNamespaceOperand {
    public final DirectoryLayer directoryLayer = new DirectoryLayer(true);
    public final FoundationDBService service;
    public final NamespaceMessage namespaceMessage;
    public final Transaction transaction;
    public final Response response;
    public CompletableFuture<DirectorySubspace> future;

    BaseNamespaceOperand(FoundationDBService service, NamespaceMessage namespaceMessage, Response response) {
        this.service = service;
        this.namespaceMessage = namespaceMessage;
        this.response = response;

        Channel channel = response.getContext().channel();
        Attribute<Transaction> transactionAttr = channel.attr(ChannelAttributes.TRANSACTION);
        Attribute<Boolean> beginAttr = channel.attr(ChannelAttributes.BEGIN);
        if (beginAttr.get() == null || Boolean.FALSE.equals(beginAttr.get())) {
            transaction = service.getContext().getFoundationDB().createTransaction();
            Attribute<Boolean> oneOffTransactionAttr = channel.attr(ChannelAttributes.ONE_OFF_TRANSACTION);
            transactionAttr.set(transaction);
            oneOffTransactionAttr.set(true);
        } else {
            transaction = transactionAttr.get();
        }
    }

    DirectoryLayout getBaseSubpath() {
        return DirectoryLayout.Builder.clusterName(service.getContext().getClusterName()).namespaces();
    }
}
