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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.volume.*;
import com.kronotop.volume.changelog.ChangeLog;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class PruneChangeLogSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    public PruneChangeLogSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        PruneChangeLogParameters parameters = new PruneChangeLogParameters(request.getParams());
        runAsync(context, response, () -> {
            if (parameters.retentionPeriod <= 0) {
                throw new IllegalArgumentException("retention period must be greater than zero");
            }

            DirectorySubspace subspace = service.openSubspace(parameters.volumeName);
            long cutoffStart = 0; // Start from the beginning
            long cutoffEnd = ChangeLog.calculateCutoffEnd(context, parameters.retentionPeriod);

            ChangeLog changeLog = new ChangeLog(context, subspace);
            TransactionUtils.executeThenCommit(context, (tr) -> {
                VolumeMetadata metadata = VolumeMetadata.load(tr, subspace);
                Map<Long, Long> maxPositions = new LinkedHashMap<>();
                for (Long segmentId : metadata.getSegments()) {
                    SegmentTailPointer pointer = SegmentUtil.locateTailPointer(tr, subspace, segmentId);
                    maxPositions.put(segmentId, pointer.position());
                }
                changeLog.prune(tr, cutoffStart, cutoffEnd, maxPositions);
                return null;
            });
        }, response::writeOK);
    }

    private static class PruneChangeLogParameters {
        private final String volumeName;
        private final Long retentionPeriod;

        private PruneChangeLogParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }

            volumeName = ProtocolMessageUtil.readAsString(params.get(1));
            retentionPeriod = ProtocolMessageUtil.readAsLong(params.get(2));
        }
    }
}
