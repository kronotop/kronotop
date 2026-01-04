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
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.volume.SegmentTailPointer;
import com.kronotop.volume.SegmentUtil;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.changelog.ChangeLog;
import com.kronotop.volume.handlers.protocol.SegmentTailPointerMessage;

import java.util.ArrayList;
import java.util.List;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

// The SEGMENT.TAILPOINTER command attempts to determine:
// * The exact sequence number for the active segment.
// * The closest (greatest) sequence number for sealed segments.
// Returns 0 if the segment contains no data.

@Command(SegmentTailPointerMessage.COMMAND)
@MinimumParameterCount(SegmentTailPointerMessage.MINIMUM_PARAMETER_COUNT)
public class SegmentTailPointerHandler extends BaseVolumeHandler implements Handler {

    public SegmentTailPointerHandler(VolumeService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SEGMENTTAILPOINTER).set(new SegmentTailPointerMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            SegmentTailPointerMessage message = request.attr(MessageTypes.SEGMENTTAILPOINTER).get();

            DirectorySubspace volumeSubspace = service.openSubspace(message.getVolume());

            long segmentId = message.getSegmentId();
            List<RedisMessage> result = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                // locateTailPointer returns the next valid position within the segment.
                // A returned value indicates that no user data exists beyond this point.
                // It serves as an upper bound for replication and backup processes.
                SegmentTailPointer pointer = SegmentUtil.locateTailPointer(tr, volumeSubspace, segmentId);
                long sequenceNumber = -1;

                long activeSegmentId = SegmentUtil.findActiveSegmentId(tr, volumeSubspace);
                if (activeSegmentId == segmentId) {
                    if (pointer.nextPosition() > 0) {
                        sequenceNumber = ChangeLog.resolveTailSequenceNumber(tr, volumeSubspace, segmentId, pointer);
                    }
                }
                // sequence number is -1(invalid) if the segment has data, but it's not the active segment.
                result.add(new IntegerRedisMessage(pointer.nextPosition()));
                result.add(new IntegerRedisMessage(sequenceNumber));
            }
            return result;

        }, response::writeArray);
    }
}
