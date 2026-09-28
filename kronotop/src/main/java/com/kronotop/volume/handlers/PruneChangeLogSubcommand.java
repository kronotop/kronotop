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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.handlers.InvalidNumberOfArgumentsException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.*;
import com.kronotop.volume.changelog.ChangeLog;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class PruneChangeLogSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    public PruneChangeLogSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        PruneChangeLogArguments arguments = new PruneChangeLogArguments(request.getArguments());
        runAsync(context, response, () -> {
            if (arguments.retentionPeriod <= 0) {
                throw new IllegalArgumentException("retention period must be greater than zero");
            }

            DirectorySubspace subspace = service.openSubspace(arguments.volumeName);
            long cutoffStart = 0; // Start from the beginning
            long cutoffEnd = ChangeLog.calculateCutoffEnd(context, arguments.retentionPeriod);

            ChangeLog changeLog = new ChangeLog(context, subspace);
            TransactionUtil.executeThenCommit(context, (tr) -> {
                List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, new VolumeSubspace(subspace));
                Map<Long, Long> maxPositions = new LinkedHashMap<>();
                for (Long segmentId : segmentIds) {
                    SegmentTailPointer pointer = SegmentSubspaceUtil.locateTailPointer(tr, subspace, segmentId);
                    maxPositions.put(segmentId, pointer.position());
                }
                changeLog.prune(tr, cutoffStart, cutoffEnd, maxPositions);
                return null;
            });
        }, response::writeOK);
    }

    private static class PruneChangeLogArguments {
        private final String volumeName;
        private final Long retentionPeriod;

        private PruneChangeLogArguments(ArrayList<ByteBuf> args) {
            if (args.size() != 3) {
                throw new InvalidNumberOfArgumentsException();
            }

            volumeName = ProtocolMessageUtil.readAsString(args.get(1));
            retentionPeriod = ProtocolMessageUtil.readAsLong(args.get(2));
        }
    }
}
