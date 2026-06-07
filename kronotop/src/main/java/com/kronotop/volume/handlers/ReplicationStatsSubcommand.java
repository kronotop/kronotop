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
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.IntegerRedisMessage;
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

class ReplicationStatsSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    private static final byte[] STAGE_BYTES = "stage".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATUS_BYTES = "status".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ERROR_MESSAGE_BYTES = "error_message".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CURSOR_SEGMENT_ID_BYTES = "cursor_segment_id".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CURSOR_POSITION_BYTES = "cursor_position".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CDC_SEQUENCE_NUMBER_BYTES = "cdc_sequence_number".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CDC_POSITION_BYTES = "cdc_position".getBytes(StandardCharsets.UTF_8);

    ReplicationStatsSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        ReplicationParameters parameters = new ReplicationParameters(request.getParams());

        supplyAsync(context, response, () -> {
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                String volumeName = VolumeNames.format(parameters.shardKind, parameters.shardId);
                DirectorySubspace standbySubspace = ReplicationUtil.openStandbySubspace(
                        context, tr, parameters.shardKind, volumeName, parameters.standbyId
                );
                ReplicationStatusInfo info = ReplicationUtil.readReplicationStatusInfo(tr, standbySubspace);

                Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
                result.put(wrapBytes(STAGE_BYTES), bulkString(info.stage() != null ? info.stage().name() : ""));
                result.put(wrapBytes(STATUS_BYTES), bulkString(info.status() != null ? info.status().name() : ""));
                result.put(wrapBytes(ERROR_MESSAGE_BYTES), bulkString(info.errorMessage() != null ? info.errorMessage() : ""));
                result.put(wrapBytes(CURSOR_SEGMENT_ID_BYTES), new IntegerRedisMessage(info.cursor().segmentId()));
                result.put(wrapBytes(CURSOR_POSITION_BYTES), new IntegerRedisMessage(info.cursor().position()));
                result.put(wrapBytes(CDC_SEQUENCE_NUMBER_BYTES), new IntegerRedisMessage(info.cdcStageInfo().sequenceNumber()));
                result.put(wrapBytes(CDC_POSITION_BYTES), new IntegerRedisMessage(info.cdcStageInfo().position()));
                return result;
            }
        }, response::writeMap);
    }

    private class ReplicationParameters {
        private final ShardKind shardKind;
        private final int shardId;
        private final String standbyId;

        private ReplicationParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 5) {
                throw new InvalidNumberOfParametersException();
            }

            shardKind = ProtocolMessageUtil.readShardKind(params.get(2));
            shardId = ProtocolMessageUtil.readShardId(service.getContext().getShardRegistry(), shardKind, params.get(3));
            standbyId = ProtocolMessageUtil.readMemberId(service.getContext(), params.get(4));
        }
    }
}
