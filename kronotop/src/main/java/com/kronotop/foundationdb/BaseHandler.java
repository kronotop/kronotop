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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.core.KronotopService;
import com.kronotop.server.resp.ChannelAttributes;
import com.kronotop.server.resp.NamespaceNotOpenException;
import com.kronotop.server.resp.Response;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

import java.util.concurrent.ConcurrentMap;

public class BaseHandler {
    KronotopService service;
    FoundationDBService foundationDBService;
    Database database;

    public BaseHandler(KronotopService service) {
        this.service = service;
        this.foundationDBService = service.
                getContext().
                getService(FoundationDBService.NAME);
        this.database = service.getContext().getFoundationDB();
    }

    public DirectorySubspace getNamespace(Response response, String namespace) {
        ConcurrentMap<String, DirectorySubspace> namespaces = response.
                getContext().
                channel().
                attr(ChannelAttributes.OPEN_NAMESPACES).get();
        DirectorySubspace directorySubspace = namespaces.get(namespace);
        if (directorySubspace == null) {
            throw new NamespaceNotOpenException(String.format("namespace '%s' not open", namespace));
        }
        return directorySubspace;
    }

    public Transaction getOrCreateTransaction(Response response) {
        Channel channel = response.getContext().channel();
        Attribute<Transaction> transactionAttr = channel.attr(ChannelAttributes.TRANSACTION);
        Attribute<Boolean> beginAttr = channel.attr(ChannelAttributes.BEGIN);

        if (beginAttr.get() == null || Boolean.FALSE.equals(beginAttr.get())) {
            Attribute<Boolean> oneOffTransactionAttr = channel.attr(ChannelAttributes.ONE_OFF_TRANSACTION);
            Transaction tx = database.createTransaction();
            transactionAttr.set(tx);
            oneOffTransactionAttr.set(true);
            return tx;
        }
        return transactionAttr.get();
    }

    public Boolean isOneOff(Response response) {
        Channel channel = response.getContext().channel();
        Attribute<Boolean> oneOffTransactionAttr = channel.attr(ChannelAttributes.ONE_OFF_TRANSACTION);
        return oneOffTransactionAttr.get() != null && !Boolean.FALSE.equals(oneOffTransactionAttr.get());
    }

    public Boolean isSnapshotRead(Response response) {
        Channel channel = response.getContext().channel();
        Attribute<Boolean> snapshotReadAttr = channel.attr(ChannelAttributes.SNAPSHOT_READ);
        return snapshotReadAttr.get() != null && !Boolean.FALSE.equals(snapshotReadAttr.get());
    }
}
