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


package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.KronotopException;
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.task.TaskService;
import com.kronotop.volume.MarkStalePrefixesTask;
import com.kronotop.volume.VolumeService;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

import static com.kronotop.AsyncCommandExecutor.runAsync;

public class MarkStalePrefixesSubcommand extends BaseSubcommandHandler implements SubcommandHandler {

    public MarkStalePrefixesSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        MarkStalePrefixesParameters parameters = new MarkStalePrefixesParameters(request.getParams());

        runAsync(context, response, () -> {
            TaskService taskService = context.getService(TaskService.NAME);
            if (parameters.operation.equals(MarkStalePrefixesParameters.Operation.START)) {
                if (taskService.hasTask(MarkStalePrefixesTask.NAME)) {
                    throw new KronotopException("Task " + MarkStalePrefixesTask.NAME + " already exists");
                }
                MarkStalePrefixesTask task = new MarkStalePrefixesTask(context);
                taskService.execute(task);
            } else if (parameters.operation.equals(MarkStalePrefixesParameters.Operation.STOP)) {
                taskService.shutdownAndRemoveTask(MarkStalePrefixesTask.NAME);
            } else if (parameters.operation.equals(MarkStalePrefixesParameters.Operation.REMOVE)) {
                taskService.shutdownAndRemoveTask(MarkStalePrefixesTask.NAME);
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    KronotopDirectoryNode node = KronotopDirectory.
                            kronotop().
                            cluster(context.getClusterName()).
                            metadata().
                            tasks().
                            task(MarkStalePrefixesTask.NAME);
                    DirectoryLayer.getDefault().removeIfExists(tr, node.toList()).join();
                    tr.commit().join();
                }
            }
        }, response::writeOK);
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
            START, STOP, REMOVE
        }
    }
}
