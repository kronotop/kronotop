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

import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.DoubleRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.segment.SegmentAnalysis;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.bulkString;
import static com.kronotop.server.RESPUtil.wrapBytes;

class DescribeSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    private static final byte[] NAME_BYTES = "name".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATUS_BYTES = "status".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA_DIR_BYTES = "data_dir".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEGMENT_SIZE_BYTES = "segment_size".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEGMENTS_BYTES = "segments".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SIZE_BYTES = "size".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FREE_BYTES_BYTES = "free_bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] USED_BYTES_BYTES = "used_bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] GARBAGE_PERCENTAGE_BYTES = "garbage_percentage".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CARDINALITY_BYTES = "cardinality".getBytes(StandardCharsets.UTF_8);

    public DescribeSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        DescribeParameters parameters = new DescribeParameters(request.getParams());

        supplyAsync(context, response, () -> {
            Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
            Volume volume = service.findVolume(parameters.name);
            VolumeConfig config = volume.getConfig();

            result.put(wrapBytes(NAME_BYTES), bulkString(config.name()));
            result.put(wrapBytes(STATUS_BYTES), bulkString(volume.getStatus().name()));
            result.put(wrapBytes(DATA_DIR_BYTES), bulkString(config.dataDir()));
            result.put(wrapBytes(SEGMENT_SIZE_BYTES), new IntegerRedisMessage(config.segmentSize()));

            List<SegmentAnalysis> segments = volume.analyze();
            Map<RedisMessage, RedisMessage> segmentAnalysis = new LinkedHashMap<>();
            for (SegmentAnalysis analysis : segments) {
                Map<RedisMessage, RedisMessage> segment = new LinkedHashMap<>();
                segment.put(wrapBytes(SIZE_BYTES), new IntegerRedisMessage(analysis.size()));
                segment.put(wrapBytes(FREE_BYTES_BYTES), new IntegerRedisMessage(analysis.freeBytes()));
                segment.put(wrapBytes(USED_BYTES_BYTES), new IntegerRedisMessage(analysis.usedBytes()));
                segment.put(wrapBytes(GARBAGE_PERCENTAGE_BYTES), new DoubleRedisMessage(analysis.garbagePercentage()));
                segment.put(wrapBytes(CARDINALITY_BYTES), new IntegerRedisMessage(analysis.cardinality()));

                segmentAnalysis.put(new IntegerRedisMessage(analysis.segmentId()), new MapRedisMessage(segment));
            }
            result.put(wrapBytes(SEGMENTS_BYTES), new MapRedisMessage(segmentAnalysis));
            return result;
        }, response::writeMap);
    }

    private static class DescribeParameters {
        private final String name;

        private DescribeParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 2) {
                throw new InvalidNumberOfParametersException();
            }

            name = ProtocolMessageUtil.readAsString(params.get(1));
        }
    }
}
