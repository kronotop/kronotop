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
import com.kronotop.server.RESPError;
import com.kronotop.foundationdb.protocol.GetApproximateSizeMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

import java.util.concurrent.CompletableFuture;

@Command(GetApproximateSizeMessage.COMMAND)
@MaximumParameterCount(GetApproximateSizeMessage.MAXIMUM_PARAMETER_COUNT)
class GetApproximateSizeHandler extends BaseFoundationDBHandler implements Handler {

    GetApproximateSizeHandler(FoundationDBService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.GETAPPROXIMATESIZE).set(new GetApproximateSizeMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        Channel channel = response.getCtx().channel();
        Attribute<Boolean> beginAttr = channel.attr(SessionAttributes.BEGIN);
        if (!Boolean.TRUE.equals(beginAttr.get())) {
            response.writeError(RESPError.TRANSACTION, "there is no transaction in progress.");
            return;
        }

        Attribute<Transaction> transactionAttr = channel.attr(SessionAttributes.TRANSACTION);
        Transaction tr = transactionAttr.get();
        CompletableFuture<Long> future = tr.getApproximateSize();
        Long size = future.join();
        response.writeInteger(size);
    }
}

