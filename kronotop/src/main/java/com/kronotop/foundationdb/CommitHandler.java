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
import com.kronotop.foundationdb.protocol.CommitMessage;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

@Command(CommitMessage.COMMAND)
@MaximumParameterCount(CommitMessage.MAXIMUM_PARAMETER_COUNT)
class CommitHandler implements Handler {
    private final FoundationDBService service;

    CommitHandler(FoundationDBService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.COMMIT).set(new CommitMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        // Validates the request
        CommitMessage commitMessage = request.attr(MessageTypes.COMMIT).get();

        Channel channel = response.getContext().channel();
        Attribute<Boolean> beginAttr = channel.attr(ChannelAttributes.BEGIN);
        if (beginAttr.get() == null || Boolean.FALSE.equals(beginAttr.get())) {
            response.writeError(RESPError.TRANSACTION, "there is no transaction in progress.");
            return;
        }

        Attribute<Transaction> transactionAttr = channel.attr(ChannelAttributes.TRANSACTION);
        try (Transaction tr = transactionAttr.get()) {
            tr.commit().join();

            String operand = commitMessage.getOperand();
            if (operand == null) {
                response.writeOK();
                return;
            }

            if (operand.equalsIgnoreCase(CommitMessage.GET_COMMITTED_VERSION_OPERAND)) {
                Long committedVersion = tr.getCommittedVersion();
                response.writeInteger(committedVersion);
            } else if (operand.equalsIgnoreCase(CommitMessage.GET_VERSIONSTAMP_OPERAND)) {
                byte[] versionstamp = tr.getVersionstamp().join();
                ByteBuf buf = response.getContext().alloc().buffer();
                buf.writeBytes(versionstamp);
                response.write(buf);
            } else {
                throw new UnknownOperandException(String.format("unknown operand: %s", operand));
            }
        } finally {
            beginAttr.set(false);
            transactionAttr.set(null);
        }
    }
}