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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.NamespaceUtils;
import com.kronotop.TransactionUtils;
import com.kronotop.VersionstampUtils;
import com.kronotop.common.resp.RESPError;
import com.kronotop.foundationdb.protocol.CommitMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.resp3.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

import java.util.*;
import java.util.concurrent.CompletableFuture;

@Command(CommitMessage.COMMAND)
@MaximumParameterCount(CommitMessage.MAXIMUM_PARAMETER_COUNT)
class CommitHandler extends BaseHandler implements Handler {

    CommitHandler(FoundationDBService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.COMMIT).set(new CommitMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        // Validates the request
        CommitMessage message = request.attr(MessageTypes.COMMIT).get();

        Channel channel = response.getChannelContext().channel();
        Attribute<Boolean> beginAttr = channel.attr(ChannelAttributes.BEGIN);
        if (beginAttr.get() == null || Boolean.FALSE.equals(beginAttr.get())) {
            response.writeError(RESPError.TRANSACTION, "there is no transaction in progress.");
            return;
        }

        Attribute<Transaction> transactionAttr = channel.attr(ChannelAttributes.TRANSACTION);
        try (Transaction tr = transactionAttr.get()) {
            CompletableFuture<byte[]> versionstamp;
            if (message.getReturning().contains(CommitMessage.Parameter.VERSIONSTAMP)) {
                versionstamp = tr.getVersionstamp();
            } else if (message.getReturning().contains(CommitMessage.Parameter.FUTURES)) {
                versionstamp = tr.getVersionstamp();
            } else {
                // Effectively final
                versionstamp = null;
            }

            tr.commit().join();

            TransactionUtils.runPostCommitHooks(request.getChannelContext());

            if (message.getReturning().isEmpty()) {
                response.writeOK();
                return;
            }

            List<RedisMessage> children = new ArrayList<>();
            message.getReturning().forEach(parameter -> {
                switch (parameter) {
                    case COMMITTED_VERSION -> {
                        Long committedVersion = tr.getCommittedVersion();
                        children.add(new IntegerRedisMessage(committedVersion));
                    }
                    case VERSIONSTAMP -> {
                        assert versionstamp != null;
                        byte[] versionBytes = versionstamp.join();
                        String encoded = VersionstampUtils.base32HexEncode(Versionstamp.complete(versionBytes));
                        ByteBuf buf = response.getChannelContext().alloc().buffer();
                        buf.writeBytes(encoded.getBytes());
                        children.add(new FullBulkStringRedisMessage(buf));
                    }
                    case FUTURES -> {
                        assert versionstamp != null;
                        byte[] versionBytes = versionstamp.join();
                        Map<RedisMessage, RedisMessage> futures = new HashMap<>();
                        List<Integer> asyncReturning = request.getChannelContext().channel().attr(ChannelAttributes.ASYNC_RETURNING).get();
                        if (asyncReturning != null) {
                            for (Integer userVersion : asyncReturning) {
                                Versionstamp completed = Versionstamp.complete(versionBytes, userVersion);
                                ByteBuf buf = request.getChannelContext().alloc().buffer();
                                buf.writeBytes(VersionstampUtils.base32HexEncode(completed).getBytes());
                                futures.put(new SimpleStringRedisMessage(String.format("$%d", userVersion)), new FullBulkStringRedisMessage(buf));
                            }
                        }
                        children.add(new MapRedisMessage(futures));
                    }
                    default -> throw new UnknownSubcommandException(parameter.getValue());
                }
                response.writeArray(children);
            });
        } finally {
            beginAttr.set(false);
            transactionAttr.set(null);
            channel.attr(ChannelAttributes.TRANSACTION_USER_VERSION).set(0);
            channel.attr(ChannelAttributes.POST_COMMIT_HOOKS).set(new LinkedList<>());
            NamespaceUtils.clearOpenNamespaces(request.getChannelContext());
        }
    }
}