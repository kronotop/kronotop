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


package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.net.HostAndPort;
import com.kronotop.KronotopException;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberNotRegisteredException;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.network.Address;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.task.TaskService;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.MarkStalePrefixesTask;
import com.kronotop.volume.VolumeService;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.kronotop.AsyncCommandExecutor.runAsync;
import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.bulkString;

public class MarkStalePrefixesSubcommand extends BaseSubcommandHandler implements SubcommandHandler {

    public MarkStalePrefixesSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        MarkStalePrefixesParameters parameters = new MarkStalePrefixesParameters(request.getParams());

        if (parameters.operation.equals(MarkStalePrefixesParameters.Operation.LOCATE)) {
            supplyAsync(context, response, () -> {
                KronotopDirectoryNode node = KronotopDirectory.kronotop()
                        .cluster(context.getClusterName()).metadata().tasks()
                        .task(MarkStalePrefixesTask.NAME);
                try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                    DirectorySubspace subspace;
                    try {
                        subspace = context.getDirectoryLayer().open(tr, node.toList()).join();
                    } catch (CompletionException e) {
                        if (e.getCause() instanceof NoSuchDirectoryException) {
                            throw new KronotopException("no metadata found");
                        }
                        throw e;
                    }
                    byte[] memberIdBytes = tr.get(subspace.pack(MarkStalePrefixesTask.METADATA_KEY.MEMBER_ID.name())).join();
                    byte[] processIdBytes = tr.get(subspace.pack(MarkStalePrefixesTask.METADATA_KEY.PROCESS_ID.name())).join();
                    if (memberIdBytes == null) {
                        throw new KronotopException("no metadata found");
                    }

                    String memberId = new String(memberIdBytes);
                    Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
                    result.put(bulkString("member_id"), bulkString(memberId));

                    Versionstamp processId = Versionstamp.fromBytes(processIdBytes);
                    result.put(bulkString("process_id"), bulkString(VersionstampUtil.base32HexEncode(processId)));

                    String externalAddress = null;
                    String internalAddress = null;
                    try {
                        MembershipService membership = context.getService(MembershipService.NAME);
                        Member member = membership.findMember(memberId);

                        Address internal = member.getInternalAddress();
                        internalAddress = HostAndPort.fromParts(internal.getHost(), internal.getPort()).toString();

                        Address external = member.getExternalAddress();
                        externalAddress = HostAndPort.fromParts(external.getHost(), external.getPort()).toString();
                    } catch (MemberNotRegisteredException ignored) {
                    }

                    result.put(bulkString("external_address"), bulkString(externalAddress));
                    result.put(bulkString("internal_address"), bulkString(internalAddress));

                    return result;
                }
            }, response::writeMap);
        } else {
            runAsync(context, response, () -> {
                TaskService taskService = context.getService(TaskService.NAME);
                if (parameters.operation.equals(MarkStalePrefixesParameters.Operation.START)) {
                    MarkStalePrefixesTask task = new MarkStalePrefixesTask(context);
                    taskService.execute(task);
                } else if (parameters.operation.equals(MarkStalePrefixesParameters.Operation.STOP)) {
                    taskService.shutdownAndRemoveTask(MarkStalePrefixesTask.NAME);
                } else if (parameters.operation.equals(MarkStalePrefixesParameters.Operation.REMOVE)) {
                    taskService.shutdownAndRemoveTask(MarkStalePrefixesTask.NAME);
                    MarkStalePrefixesTask.removeMetadata(context);
                }
            }, response::writeOK);
        }
    }

    private static class MarkStalePrefixesParameters {
        private final Operation operation;

        private MarkStalePrefixesParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 2) {
                throw new InvalidNumberOfParametersException();
            }

            String tmp = ProtocolMessageUtil.readAsString(params.get(1));
            try {
                operation = Operation.valueOf(tmp.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("invalid operation: " + tmp);
            }
        }

        enum Operation {
            START, STOP, REMOVE, LOCATE
        }
    }
}
