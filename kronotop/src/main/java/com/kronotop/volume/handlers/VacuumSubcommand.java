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

import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketEntryEvacuator;
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.UnknownSubcommandException;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.*;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.runAsync;
import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.wrapBytes;

class VacuumSubcommand extends BaseSubcommandHandler implements SubcommandHandler {

    private static final byte[] ACTIVE_BYTES = "active".getBytes(StandardCharsets.UTF_8);
    private static final byte[] METADATA_BYTES = "metadata".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEGMENTS_BYTES = "segments".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEGMENT_ID_BYTES = "segment_id".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATUS_BYTES = "status".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STARTED_AT_BYTES = "started_at".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMPLETED_AT_BYTES = "completed_at".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RESULT_BYTES = "result".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEGMENTS_PROCESSED_BYTES = "segments_processed".getBytes(StandardCharsets.UTF_8);

    VacuumSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        ArrayList<ByteBuf> params = request.getParams();
        if (params.size() < 2) {
            throw new InvalidNumberOfParametersException();
        }
        String operation = ProtocolMessageUtil.readAsString(params.get(1)).toUpperCase();
        switch (operation) {
            case "START" -> vacuumStart(request, response);
            case "STOP" -> vacuumStop(request, response);
            case "DROP" -> vacuumDrop(request, response);
            case "STATUS" -> vacuumStatus(request, response);
            default -> throw new UnknownSubcommandException(operation);
        }
    }

    private void vacuumStart(Request request, Response response) {
        StartParameters parameters = new StartParameters(request.getParams());
        runAsync(context, response, () -> {
            Volume volume = service.findVolume(parameters.volumeName);
            VolumeNames.Parsed parsed = VolumeNames.parse(parameters.volumeName);
            volume.vacuumStart(
                    parameters.garbageThreshold,
                    () -> new BucketEntryEvacuator(volume, parsed.shardId())
            );
        }, response::writeOK);
    }

    private void vacuumStop(Request request, Response response) {
        Parameters parameters = new Parameters(request.getParams());
        runAsync(context, response, () -> {
            service.findVolume(parameters.volumeName).vacuumStop();
        }, response::writeOK);
    }

    private void vacuumDrop(Request request, Response response) {
        Parameters parameters = new Parameters(request.getParams());
        runAsync(context, response, () -> {
            service.findVolume(parameters.volumeName).vacuumDrop();
        }, response::writeOK);
    }

    private void vacuumStatus(Request request, Response response) {
        Parameters parameters = new Parameters(request.getParams());
        supplyAsync(context, response, () -> {
            Volume volume = service.findVolume(parameters.volumeName);
            VacuumStatusResult status = volume.vacuumStatus();

            List<RedisMessage> segmentList = new ArrayList<>();
            for (VacuumSegmentMetadata metadata : status.segments()) {
                Map<RedisMessage, RedisMessage> segmentMap = new LinkedHashMap<>();
                segmentMap.put(wrapBytes(SEGMENT_ID_BYTES), new IntegerRedisMessage(metadata.segmentId()));
                segmentMap.put(wrapBytes(STATUS_BYTES), wrapBytes(metadata.status().name().getBytes(StandardCharsets.UTF_8)));
                segmentMap.put(wrapBytes(STARTED_AT_BYTES), new IntegerRedisMessage(metadata.startedAt()));
                segmentList.add(new MapRedisMessage(segmentMap));
            }

            Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
            result.put(wrapBytes(ACTIVE_BYTES), status.active() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE);

            if (status.metadata() != null) {
                VacuumMetadata metadata = status.metadata();
                Map<RedisMessage, RedisMessage> metadataMap = new LinkedHashMap<>();
                metadataMap.put(wrapBytes(STARTED_AT_BYTES), new IntegerRedisMessage(metadata.startedAt()));
                metadataMap.put(wrapBytes(COMPLETED_AT_BYTES), new IntegerRedisMessage(metadata.completedAt()));
                metadataMap.put(wrapBytes(RESULT_BYTES), wrapBytes(metadata.result().name().getBytes(StandardCharsets.UTF_8)));
                metadataMap.put(wrapBytes(SEGMENTS_PROCESSED_BYTES), new IntegerRedisMessage(metadata.segmentsProcessed()));
                result.put(wrapBytes(METADATA_BYTES), new MapRedisMessage(metadataMap));
            }

            result.put(wrapBytes(SEGMENTS_BYTES), new ArrayRedisMessage(segmentList));
            return result;
        }, response::writeMap);
    }

    private static class Parameters {
        private final String volumeName;

        private Parameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }
            volumeName = ProtocolMessageUtil.readAsString(params.get(2));
        }
    }

    private static class StartParameters {
        private final String volumeName;
        private final float garbageThreshold;

        private StartParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 4) {
                throw new InvalidNumberOfParametersException();
            }
            volumeName = ProtocolMessageUtil.readAsString(params.get(2));

            try {
                garbageThreshold = Float.parseFloat(ProtocolMessageUtil.readAsString(params.get(3)));
            } catch (NumberFormatException e) {
                throw new KronotopException("garbage-threshold must be a number");
            }
            if (garbageThreshold <= 0 || garbageThreshold >= 100) {
                throw new KronotopException("garbage-threshold must be between 0 and 100 (exclusive)");
            }
        }
    }
}
