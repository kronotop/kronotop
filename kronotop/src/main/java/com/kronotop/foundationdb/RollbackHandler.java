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

package com.kronotop.foundationdb;

import com.apple.foundationdb.Transaction;
import com.kronotop.NamespaceUtils;
import com.kronotop.common.resp.RESPError;
import com.kronotop.foundationdb.protocol.RollbackMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

import java.util.LinkedList;

@Command(RollbackMessage.COMMAND)
@MaximumParameterCount(RollbackMessage.MAXIMUM_PARAMETER_COUNT)
class RollbackHandler implements Handler {
    private final FoundationDBService service;

    RollbackHandler(FoundationDBService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ROLLBACK).set(new RollbackMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        Channel channel = response.getChannelContext().channel();

        Attribute<Boolean> beginAttr = channel.attr(ChannelAttributes.BEGIN);
        if (beginAttr.get() == null || Boolean.FALSE.equals(beginAttr.get())) {
            response.writeError(RESPError.TRANSACTION, "there is no transaction in progress.");
            return;
        }

        Attribute<Transaction> transactionAttr = channel.attr(ChannelAttributes.TRANSACTION);
        Transaction tr = transactionAttr.get();
        try {
            tr.cancel();
        } finally {
            beginAttr.set(false);
            transactionAttr.set(null);
            channel.attr(ChannelAttributes.TRANSACTION_USER_VERSION).set(0);
            channel.attr(ChannelAttributes.POST_COMMIT_HOOKS).set(new LinkedList<>());
            NamespaceUtils.clearOpenNamespaces(request.getChannelContext());
        }

        response.writeOK();
    }
}