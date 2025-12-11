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
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.SegmentTailPointer;
import com.kronotop.volume.SegmentUtil;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.changelog.ChangeLog;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

public class CursorSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    public CursorSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        CursorParameters parameters = new CursorParameters(request.getParams());

        supplyAsync(context, response, () -> {
            DirectorySubspace volumeSubspace = service.openSubspace(parameters.volumeName);
            Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                long activeSegmentId = SegmentUtil.findActiveSegmentId(tr, volumeSubspace);
                SegmentTailPointer pointer = SegmentUtil.locateTailPointer(tr, volumeSubspace, activeSegmentId);
                long sequenceNumber = ChangeLog.resolveTailSequenceNumber(tr, volumeSubspace, activeSegmentId, pointer);
                result.put(
                        new SimpleStringRedisMessage("active_segment_id"),
                        new IntegerRedisMessage(activeSegmentId)
                );
                result.put(
                        new SimpleStringRedisMessage("versionstamp"),
                        new SimpleStringRedisMessage(pointer.versionstamp() != null ?
                                VersionstampUtil.base32HexEncode(pointer.versionstamp()) : "")
                );
                result.put(
                        new SimpleStringRedisMessage("next_position"),
                        new IntegerRedisMessage(pointer.nextPosition())
                );
                result.put(
                        new SimpleStringRedisMessage("sequence_number"),
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
