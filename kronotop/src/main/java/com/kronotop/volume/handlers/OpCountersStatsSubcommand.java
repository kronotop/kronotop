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
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.VolumeStats;
import com.kronotop.volume.handlers.protocol.VolumeStatsMessage;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.wrapBytes;

class OpCountersStatsSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    private static final byte[] APPENDS_BYTES = "appends".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DELETES_BYTES = "deletes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] UPDATES_BYTES = "updates".getBytes(StandardCharsets.UTF_8);
    private static final byte[] GETS_BYTES = "gets".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_APPENDED_BYTES = "bytes_appended".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_READ_BYTES = "bytes_read".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEGMENTS_CREATED_BYTES = "segments_created".getBytes(StandardCharsets.UTF_8);

    OpCountersStatsSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        VolumeStatsMessage message = request.attr(MessageTypes.VOLUMESTATS).get();

        supplyAsync(context, response, () -> {
            Volume volume = service.findVolume(message.getVolumeName());
            VolumeStats stats = volume.getStats();

            Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
            result.put(wrapBytes(APPENDS_BYTES), new IntegerRedisMessage(stats.getAppends()));
            result.put(wrapBytes(DELETES_BYTES), new IntegerRedisMessage(stats.getDeletes()));
            result.put(wrapBytes(UPDATES_BYTES), new IntegerRedisMessage(stats.getUpdates()));
            result.put(wrapBytes(GETS_BYTES), new IntegerRedisMessage(stats.getGets()));
            result.put(wrapBytes(BYTES_APPENDED_BYTES), new IntegerRedisMessage(stats.getBytesAppended()));
            result.put(wrapBytes(BYTES_READ_BYTES), new IntegerRedisMessage(stats.getBytesRead()));
            result.put(wrapBytes(SEGMENTS_CREATED_BYTES), new IntegerRedisMessage(stats.getSegmentsCreated()));
            return result;
        }, response::writeMap);
    }
}
