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

import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.DoubleRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.handlers.protocol.VolumeStatsMessage;
import com.kronotop.volume.segment.SegmentAnalysis;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.wrapBytes;

class SegmentsStatsSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    private static final byte[] SEGMENT_ID_BYTES = "segment_id".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SIZE_BYTES = "size_bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] USED_BYTES = "used_bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FREE_BYTES = "free_bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] GARBAGE_BYTES = "garbage_bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] GARBAGE_PERCENTAGE_BYTES = "garbage_percentage".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CARDINALITY_BYTES = "cardinality".getBytes(StandardCharsets.UTF_8);

    SegmentsStatsSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        VolumeStatsMessage message = request.attr(MessageTypes.VOLUMESTATS).get();

        supplyAsync(context, response, () -> {
            Volume volume = service.findVolume(message.getVolumeName());
            List<SegmentAnalysis> segments = volume.analyze();

            List<RedisMessage> result = new ArrayList<>();
            for (SegmentAnalysis analysis : segments) {
                long garbageBytes = (analysis.size() - analysis.freeBytes()) - analysis.usedBytes();
                Map<RedisMessage, RedisMessage> segmentMap = new LinkedHashMap<>();
                segmentMap.put(wrapBytes(SEGMENT_ID_BYTES), new IntegerRedisMessage(analysis.segmentId()));
                segmentMap.put(wrapBytes(SIZE_BYTES), new IntegerRedisMessage(analysis.size()));
                segmentMap.put(wrapBytes(USED_BYTES), new IntegerRedisMessage(analysis.usedBytes()));
                segmentMap.put(wrapBytes(FREE_BYTES), new IntegerRedisMessage(analysis.freeBytes()));
                segmentMap.put(wrapBytes(GARBAGE_BYTES), new IntegerRedisMessage(garbageBytes));
                segmentMap.put(wrapBytes(GARBAGE_PERCENTAGE_BYTES), new DoubleRedisMessage(analysis.garbagePercentage()));
                segmentMap.put(wrapBytes(CARDINALITY_BYTES), new IntegerRedisMessage(analysis.cardinality()));
                result.add(new MapRedisMessage(segmentMap));
            }
            return result;
        }, response::writeArray);
    }
}
