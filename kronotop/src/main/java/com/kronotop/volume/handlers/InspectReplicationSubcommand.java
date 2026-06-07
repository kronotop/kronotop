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
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.VolumeNames;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.replication.ReplicationStatusInfo;
import com.kronotop.volume.replication.ReplicationUtil;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.bulkString;
import static com.kronotop.server.RESPUtil.wrapBytes;

class InspectReplicationSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    private static final byte[] SEGMENT_ID_BYTES = "segment_id".getBytes(StandardCharsets.UTF_8);
    private static final byte[] POSITION_BYTES = "position".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEQUENCE_NUMBER_BYTES = "sequence_number".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TAIL_SEQUENCE_NUMBER_BYTES = "tail_sequence_number".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TAIL_NEXT_POSITION_BYTES = "tail_next_position".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STAGE_BYTES = "stage".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CURSOR_BYTES = "cursor".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATUS_BYTES = "status".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ERROR_MESSAGE_BYTES = "error_message".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CDC_STAGE_BYTES = "cdc_stage".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEGMENT_REPLICATION_STAGE_BYTES = "segment_replication_stage".getBytes(StandardCharsets.UTF_8);

    InspectReplicationSubcommand(VolumeService service) {
        super(service);
    }

    private RedisMessage mapCursor(ReplicationStatusInfo info) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(wrapBytes(SEGMENT_ID_BYTES), new IntegerRedisMessage(info.cursor().segmentId()));
        map.put(wrapBytes(POSITION_BYTES), new IntegerRedisMessage(info.cursor().position()));
        return new MapRedisMessage(map);
    }

    private RedisMessage mapCDCStage(ReplicationStatusInfo.ChangeDataCaptureStage cdcStage) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(wrapBytes(SEQUENCE_NUMBER_BYTES), new IntegerRedisMessage(cdcStage.sequenceNumber()));
        map.put(wrapBytes(POSITION_BYTES), new IntegerRedisMessage(cdcStage.position()));
        return new MapRedisMessage(map);
    }

    private RedisMessage mapSegmentReplicationStage(ReplicationStatusInfo.SegmentReplicationStage segmentStage) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(wrapBytes(TAIL_SEQUENCE_NUMBER_BYTES), new IntegerRedisMessage(segmentStage.tailSequenceNumber()));
        map.put(wrapBytes(TAIL_NEXT_POSITION_BYTES), new IntegerRedisMessage(segmentStage.tailNextPosition()));
        return new MapRedisMessage(map);
    }

    private Map<RedisMessage, RedisMessage> mapReplicationStatusInfo(ReplicationStatusInfo info) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();

        result.put(
                wrapBytes(STAGE_BYTES),
                bulkString(info.stage() != null ? info.stage().name() : "")
        );
        result.put(wrapBytes(CURSOR_BYTES), mapCursor(info));
        result.put(
                wrapBytes(STATUS_BYTES),
                bulkString(info.status() != null ? info.status().name() : "")
        );
        result.put(
                wrapBytes(ERROR_MESSAGE_BYTES),
                bulkString(info.errorMessage() != null ? info.errorMessage() : "")
        );

        result.put(wrapBytes(CDC_STAGE_BYTES), mapCDCStage(info.cdcStageInfo()));
        result.put(wrapBytes(SEGMENT_REPLICATION_STAGE_BYTES), mapSegmentReplicationStage(info.segmentReplicationInfo()));

        return result;
    }

    @Override
    public void execute(Request request, Response response) {
        ReplicationParameters parameters = new ReplicationParameters(request.getParams());

        supplyAsync(context, response, () -> {
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                VolumeNames.Parsed parsed = VolumeNames.parse(parameters.volumeName);
                DirectorySubspace standbySubspace = ReplicationUtil.openStandbySubspace(
                        context, tr, parsed.shardKind(), parameters.volumeName, parameters.standById
                );
                ReplicationStatusInfo info = ReplicationUtil.readReplicationStatusInfo(tr, standbySubspace);
                return mapReplicationStatusInfo(info);
            }
        }, response::writeMap);
    }

    private class ReplicationParameters {
        private final String volumeName;
        private final String standById;

        private ReplicationParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }

            volumeName = ProtocolMessageUtil.readAsString(params.get(1));
            standById = ProtocolMessageUtil.readMemberId(service.getContext(), params.get(2));
        }
    }
}
