/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.changelog.ChangeLog;
import com.kronotop.volume.handlers.protocol.ChangeLogWatchMessage;

import java.util.concurrent.CompletableFuture;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(ChangeLogWatchMessage.COMMAND)
@MinimumParameterCount(ChangeLogWatchMessage.MINIMUM_PARAMETER_COUNT)
public class ChangeLogWatchHandler extends BaseVolumeHandler implements Handler {
    public ChangeLogWatchHandler(VolumeService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.CHANGELOGWATCH).set(new ChangeLogWatchMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        // 1. Read the latest sequence number.
        // 2. If it's lower than the client's value, return an error.
        // 3. If it's higher, return the latest value.
        // 4. If it's equal, register a mutation watch and wait.
        // 5. When the watch fires, read the latest value again and return it.
        supplyAsync(context, response, () -> {
            ChangeLogWatchMessage message = request.attr(MessageTypes.CHANGELOGWATCH).get();
            DirectorySubspace subspace = service.openSubspace(message.getVolume());
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                long sequenceNumber = ChangeLog.getLatestSequenceNumber(tr, subspace);
                if (sequenceNumber < message.getSequenceNumber()) {
                    throw new IllegalArgumentException("Sequence number cannot be greater than the volume’s latest sequence number");
                } else if (sequenceNumber > message.getSequenceNumber()) {
                    return sequenceNumber;
                }

                // MutationWatcher.watch "may" commit the transaction if it calls setWatcher.
                //
                // The watcher must be created in the same transaction that reads the sequence number.
                // FoundationDB watches are designed for this - if you set a watch and commit, and the key was
                // already modified before the commit, the watch fires immediately.
                long volumeId = service.getVolumeId(subspace);
                byte[] mutationTrigger = service.getMutationTriggerKey(subspace);
                CompletableFuture<Void> watch = service.mutationWatcher().watch(tr, volumeId, mutationTrigger);
                watch.join();
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return ChangeLog.getLatestSequenceNumber(tr, subspace);
            }
        }, response::writeInteger);
    }
}
