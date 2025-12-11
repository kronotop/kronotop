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
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.VolumeNames;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.replication.ReplicationStatusInfo;
import com.kronotop.volume.replication.ReplicationUtil;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

public class ReplicationSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    public ReplicationSubcommand(VolumeService service) {
        super(service);
    }

    private RedisMessage mapCursor(ReplicationStatusInfo info) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(new SimpleStringRedisMessage("segment_id"), new IntegerRedisMessage(info.cursor().segmentId()));
        map.put(new SimpleStringRedisMessage("position"), new IntegerRedisMessage(info.cursor().position()));
        return new MapRedisMessage(map);
    }

    private RedisMessage mapCDCStage(ReplicationStatusInfo.ChangeDataCaptureStage cdcStage) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(new SimpleStringRedisMessage("sequence_number"), new IntegerRedisMessage(cdcStage.sequenceNumber()));
        map.put(new SimpleStringRedisMessage("position"), new IntegerRedisMessage(cdcStage.position()));
        return new MapRedisMessage(map);
    }

    private RedisMessage mapSegmentReplicationStage(ReplicationStatusInfo.SegmentReplicationStage segmentStage) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(new SimpleStringRedisMessage("tail_sequence_number"), new IntegerRedisMessage(segmentStage.tailSequenceNumber()));
        map.put(new SimpleStringRedisMessage("tail_next_position"), new IntegerRedisMessage(segmentStage.tailNextPosition()));
        return new MapRedisMessage(map);
    }

    private Map<RedisMessage, RedisMessage> mapReplicationStatusInfo(ReplicationStatusInfo info) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();

        // Root level fields
        result.put(
                new SimpleStringRedisMessage("stage"),
                new SimpleStringRedisMessage(info.stage() != null ? info.stage().name() : "")
        );
        result.put(new SimpleStringRedisMessage("cursor"), mapCursor(info));
        result.put(
                new SimpleStringRedisMessage("status"),
                new SimpleStringRedisMessage(info.status() != null ? info.status().name() : "")
        );
        result.put(
                new SimpleStringRedisMessage("error_message"),
                new SimpleStringRedisMessage(info.errorMessage() != null ? info.errorMessage() : "")
        );

        // Nested DTOs
        result.put(new SimpleStringRedisMessage("cdc_stage"), mapCDCStage(info.cdcStageInfo()));
        result.put(new SimpleStringRedisMessage("segment_replication_stage"), mapSegmentReplicationStage(info.segmentReplicationInfo()));

        return result;
    }

    @Override
    public void execute(Request request, Response response) {
        ReplicationParameters parameters = new ReplicationParameters(request.getParams());

        supplyAsync(context, response, () -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                String volumeName = VolumeNames.format(parameters.shardKind, parameters.shardId);
                DirectorySubspace standbySubspace = ReplicationUtil.openStandbySubspace(
                        context, tr, volumeName, parameters.standById
                );
                ReplicationStatusInfo info = ReplicationUtil.readReplicationStatusInfo(tr, standbySubspace);
                return mapReplicationStatusInfo(info);
            }
        }, response::writeMap);
    }

    private class ReplicationParameters {
        private final ShardKind shardKind;
        private final int shardId;
        private final String standById;

        private ReplicationParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 4) {
                throw new InvalidNumberOfParametersException();
            }

            shardKind = ProtocolMessageUtil.readShardKind(params.get(1));
            shardId = ProtocolMessageUtil.readShardId(service.getContext().getConfig(), shardKind, params.get(2));
            standById = ProtocolMessageUtil.readMemberId(service.getContext(), params.get(3));
        }
    }
}
