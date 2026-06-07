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
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.SegmentSubspaceUtil;
import com.kronotop.volume.SegmentTailPointer;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.changelog.ChangeLog;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.bulkString;
import static com.kronotop.server.RESPUtil.wrapBytes;

public class CursorSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    private static final byte[] ACTIVE_SEGMENT_ID_BYTES = "active_segment_id".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VERSIONSTAMP_BYTES = "versionstamp".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NEXT_POSITION_BYTES = "next_position".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEQUENCE_NUMBER_BYTES = "sequence_number".getBytes(StandardCharsets.UTF_8);

    public CursorSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        CursorParameters parameters = new CursorParameters(request.getParams());

        supplyAsync(context, response, () -> {
            DirectorySubspace volumeSubspace = service.openSubspace(parameters.volumeName);
            Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                long activeSegmentId = SegmentSubspaceUtil.findActiveSegmentId(tr, volumeSubspace);
                SegmentTailPointer pointer = SegmentSubspaceUtil.locateTailPointer(tr, volumeSubspace, activeSegmentId);
                long sequenceNumber = ChangeLog.resolveTailSequenceNumber(tr, volumeSubspace, activeSegmentId, pointer);
                result.put(
                        wrapBytes(ACTIVE_SEGMENT_ID_BYTES),
                        new IntegerRedisMessage(activeSegmentId)
                );
                result.put(
                        wrapBytes(VERSIONSTAMP_BYTES),
                        bulkString(pointer.versionstamp() != null ?
                                VersionstampUtil.base32HexEncode(pointer.versionstamp()) : "")
                );
                result.put(
                        wrapBytes(NEXT_POSITION_BYTES),
                        new IntegerRedisMessage(pointer.nextPosition())
                );
                result.put(
                        wrapBytes(SEQUENCE_NUMBER_BYTES),
                        new IntegerRedisMessage(sequenceNumber)
                );
            }
            return result;
        }, response::writeMap);
    }

    private static class CursorParameters {
        private final String volumeName;

        private CursorParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 2) {
                throw new InvalidNumberOfParametersException();
            }

            volumeName = ProtocolMessageUtil.readAsString(params.get(1));
        }
    }
}
