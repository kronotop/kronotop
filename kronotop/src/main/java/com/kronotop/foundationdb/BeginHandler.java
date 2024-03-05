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
import com.kronotop.common.resp.RESPError;
import com.kronotop.core.NamespaceUtils;
import com.kronotop.foundationdb.protocol.BeginMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

import java.util.LinkedList;

@Command(BeginMessage.COMMAND)
@MaximumParameterCount(BeginMessage.MAXIMUM_PARAMETER_COUNT)
class BeginHandler implements Handler {
    private final FoundationDBService service;

    BeginHandler(FoundationDBService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BEGIN).set(new BeginMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        Channel channel = request.getChannelContext().channel();
        Attribute<Boolean> beginAttr = channel.attr(ChannelAttributes.BEGIN);
        if (Boolean.TRUE.equals(beginAttr.get())) {
            response.writeError(RESPError.TRANSACTION, "there is already a transaction in progress.");
            return;
        }

        Transaction tr = service.getContext().getFoundationDB().createTransaction();
        channel.attr(ChannelAttributes.TRANSACTION).set(tr);
        channel.attr(ChannelAttributes.TRANSACTION_USER_VERSION).set(0);
        channel.attr(ChannelAttributes.POST_COMMIT_HOOKS).set(new LinkedList<>());
        beginAttr.set(true);
        NamespaceUtils.clearOpenNamespaces(request.getChannelContext());

        response.writeOK();
    }
}
