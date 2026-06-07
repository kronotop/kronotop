/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.Volume;
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
        supplyAsync(context, response, () -> {
            ChangeLogWatchMessage message = request.attr(MessageTypes.CHANGELOGWATCH).get();
            Volume volume = service.findVolume(message.getVolume());
            DirectorySubspace subspace = volume.getConfig().subspace();
            ChangeLog changeLog = volume.getChangeLog();

            long volumeId = service.getVolumeId(subspace);
            byte[] mutationTrigger = service.getMutationTriggerKey(subspace);

            while (true) {
                try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                    long latestSequenceNumber = ChangeLog.getLatestSequenceNumber(tr, subspace);
                    if (latestSequenceNumber < message.getSequenceNumber()) {
                        throw new IllegalArgumentException(
                                "Sequence number cannot be greater than the volume's latest sequence number"
                        );
                    }

                    long safeSequenceNumber = Math.min(
                            changeLog.getSafeWatermark(), latestSequenceNumber
                    );
                    if (safeSequenceNumber > message.getSequenceNumber()) {
                        return safeSequenceNumber;
                    }

                    // No safe data yet — watch for the next mutation and re-check
                    // MutationWatcher commits the transaction
                    CompletableFuture<Void> watch = service.mutationWatcher()
                            .watch(tr, volumeId, mutationTrigger);
                    watch.join();
                }
            }
        }, response::writeInteger);
    }
}
