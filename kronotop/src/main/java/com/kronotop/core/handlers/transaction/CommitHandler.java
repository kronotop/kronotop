/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.core.handlers.transaction;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.core.handlers.transaction.protocol.CommitMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.Attribute;

import java.util.concurrent.CompletableFuture;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;


@Command(CommitMessage.COMMAND)
@MaximumParameterCount(CommitMessage.MAXIMUM_PARAMETER_COUNT)
public class CommitHandler implements Handler {
    private final Context context;

    public CommitHandler(Context context) {
        this.context = context;
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.COMMIT).set(new CommitMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        supplyAsync(context, response, () -> {
            // Validates the request
            CommitMessage message = request.attr(MessageTypes.COMMIT).get();

            Session session = request.getSession();
            Attribute<Boolean> beginAttr = session.attr(SessionAttributes.BEGIN);
            if (!Boolean.TRUE.equals(beginAttr.get())) {
                throw new KronotopException(RESPError.TRANSACTION, "there is no transaction in progress.");
            }

            Attribute<Transaction> transactionAttr = session.attr(SessionAttributes.TRANSACTION);
            Transaction tr = transactionAttr.get();

            CommitMessage.Parameter returning = message.getReturning();

            CompletableFuture<byte[]> versionstamp;
            if (returning == CommitMessage.Parameter.VERSIONSTAMP) {
                versionstamp = tr.getVersionstamp();
            } else {
                // Effectively final
                versionstamp = null;
            }

            RedisMessage result;
            try {
                tr.commit().join();

                TransactionUtil.runPostCommitHooks(session);

                if (returning == null) {
                    return null;
                }

                switch (returning) {
                    case COMMITTED_VERSION -> {
                        Long committedVersion = tr.getCommittedVersion();
                        result = new IntegerRedisMessage(committedVersion);
                    }
                    case VERSIONSTAMP -> {
                        assert versionstamp != null;
                        byte[] versionBytes = versionstamp.join();
                        result = new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(versionBytes));
                    }
                    default -> throw new UnknownSubcommandException(returning.getValue());
                }
            } finally {
                session.closeAndCleanupTransaction();
            }
            return result;
        }, (result) -> {
            if (result == null) {
                response.writeOK();
            } else {
                response.writeRedisMessage(result);
            }
        });
    }
}
