/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.segment.SegmentAnalysis;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class DescribeSubcommand extends BaseHandler implements SubcommandHandler {

    public DescribeSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        DescribeParameters parameters = new DescribeParameters(request.getParams());

        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        Volume volume = service.findVolume(parameters.name);
        VolumeConfig config = volume.getConfig();

        result.put(new SimpleStringRedisMessage("name"), new SimpleStringRedisMessage(config.name()));
        result.put(new SimpleStringRedisMessage("status"), new SimpleStringRedisMessage(volume.getStatus().name()));
        result.put(new SimpleStringRedisMessage("data_dir"), new SimpleStringRedisMessage(config.dataDir()));
        result.put(new SimpleStringRedisMessage("segment_size"), new IntegerRedisMessage(config.segmentSize()));

        List<SegmentAnalysis> segments = volume.analyze();
        Map<RedisMessage, RedisMessage> segmentAnalysis = new LinkedHashMap<>();
        for (SegmentAnalysis analysis : segments) {
            Map<RedisMessage, RedisMessage> segment = new LinkedHashMap<>();
            segment.put(new SimpleStringRedisMessage("size"), new IntegerRedisMessage(analysis.size()));
            segment.put(new SimpleStringRedisMessage("free_bytes"), new IntegerRedisMessage(analysis.freeBytes()));
            segment.put(new SimpleStringRedisMessage("used_bytes"), new IntegerRedisMessage(analysis.usedBytes()));
            segment.put(new SimpleStringRedisMessage("garbage_ratio"), new DoubleRedisMessage(analysis.garbageRatio()));
            segment.put(new SimpleStringRedisMessage("cardinality"), new IntegerRedisMessage(analysis.cardinality()));

            segmentAnalysis.put(new SimpleStringRedisMessage(analysis.name()), new MapRedisMessage(segment));
        }
        result.put(new SimpleStringRedisMessage("segments"), new MapRedisMessage(segmentAnalysis));
        response.writeMap(result);
    }

    private class DescribeParameters {
        private final String name;

        private DescribeParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 2) {
                throw new InvalidNumberOfParametersException();
            }

            name = readAsString(params.get(1));
        }
    }
}
