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


package com.kronotop.cluster.handlers;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.cluster.CollectStalePrefixesTask;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberNotRegisteredException;
import com.kronotop.cluster.RoutingService;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.network.Address;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.task.TaskService;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.kronotop.AsyncCommandExecutor.runAsync;
import static com.kronotop.AsyncCommandExecutor.supplyAsync;

class CollectStalePrefixesSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    CollectStalePrefixesSubcommand(RoutingService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        CollectStalePrefixesArguments arguments = new CollectStalePrefixesArguments(request.getArguments());

        if (arguments.operation.equals(CollectStalePrefixesArguments.Operation.LOCATE)) {
            supplyAsync(context, response, () -> {
                KronotopDirectoryNode node = KronotopDirectory.kronotop()
                        .cluster(context.getClusterName()).metadata().tasks()
                        .task(CollectStalePrefixesTask.NAME);
                try (Transaction tr = context.getTransactionManager().create(request.getSession())) {
                    DirectorySubspace subspace;
                    try {
                        subspace = context.getDirectoryLayer().open(tr, node.toList()).join();
                    } catch (CompletionException e) {
                        if (e.getCause() instanceof NoSuchDirectoryException) {
                            throw new KronotopException("No metadata found");
                        }
                        throw e;
                    }
                    byte[] memberIdBytes = tr.get(subspace.pack(CollectStalePrefixesTask.METADATA_KEY.MEMBER_ID.name())).join();
                    byte[] processIdBytes = tr.get(subspace.pack(CollectStalePrefixesTask.METADATA_KEY.PROCESS_ID.name())).join();
                    if (memberIdBytes == null) {
                        throw new KronotopException("No metadata found");
                    }

                    String memberId = new String(memberIdBytes);
                    Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
                    result.put(bulkString("member_id"), bulkString(memberId));

                    Versionstamp processId = Versionstamp.fromBytes(processIdBytes);
                    result.put(bulkString("process_id"), bulkString(VersionstampUtil.base32HexEncode(processId)));

                    List<RedisMessage> externalAdvertise = new ArrayList<>();
                    List<RedisMessage> internalAdvertise = new ArrayList<>();
                    try {
                        Member member = membership.findMember(memberId);

                        for (Address address : member.getExternalAdvertise()) {
                            externalAdvertise.add(bulkString(address.toString()));
                        }
                        for (Address address : member.getInternalAdvertise()) {
                            internalAdvertise.add(bulkString(address.toString()));
                        }
                    } catch (MemberNotRegisteredException ignored) {
                    }

                    result.put(bulkString("external_advertise"), new ArrayRedisMessage(externalAdvertise));
                    result.put(bulkString("internal_advertise"), new ArrayRedisMessage(internalAdvertise));

                    return result;
                }
            }, response::writeMap);
        } else {
            runAsync(context, response, () -> {
                TaskService taskService = context.getService(TaskService.NAME);
                if (arguments.operation.equals(CollectStalePrefixesArguments.Operation.START)) {
                    CollectStalePrefixesTask task;
                    try {
                        task = new CollectStalePrefixesTask(context);
                    } catch (CompletionException e) {
                        // Two members ran START at the same time. The task metadata is written in one transaction,
                        // so the loser fails with a conflict. Report it instead of the raw FDB error.
                        if (TransactionUtil.getRootCause(e) instanceof FDBException fdbEx && fdbEx.getCode() == 1020) {
                            throw new KronotopException("Another member is starting task " + CollectStalePrefixesTask.NAME + " at the same time");
                        }
                        throw e;
                    }
                    taskService.execute(task);
                } else if (arguments.operation.equals(CollectStalePrefixesArguments.Operation.STOP)) {
                    taskService.shutdownAndRemoveTask(CollectStalePrefixesTask.NAME);
                } else if (arguments.operation.equals(CollectStalePrefixesArguments.Operation.REMOVE)) {
                    taskService.shutdownAndRemoveTask(CollectStalePrefixesTask.NAME);
                    CollectStalePrefixesTask.removeMetadata(context);
                }
            }, response::writeOK);
        }
    }

    private static class CollectStalePrefixesArguments {
        private final Operation operation;

        private CollectStalePrefixesArguments(ArrayList<ByteBuf> args) {
            if (args.size() != 2) {
                throw new InvalidNumberOfArgumentsException();
            }

            operation = ProtocolMessageUtil.readEnum(Operation.class, args.get(1), "operation");
        }

        enum Operation {
            START, STOP, REMOVE, LOCATE
        }
    }
}
