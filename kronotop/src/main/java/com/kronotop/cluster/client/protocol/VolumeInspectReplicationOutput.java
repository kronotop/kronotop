/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.cluster.client.protocol;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Lettuce output decoder for VOLUME.INSPECT REPLICATION command response.
 */
public class VolumeInspectReplicationOutput<K, V> extends CommandOutput<K, V, VolumeInspectReplicationResponse> {

    private final Deque<String> contextStack = new ArrayDeque<>();
    private String pendingKey;

    // Root level fields
    private String stage;
    private String status;
    private String errorMessage;

    // Cursor fields
    private long cursorSegmentId;
    private long cursorPosition;

    // CDC stage fields
    private long cdcSequenceNumber;
    private long cdcPosition;

    // Segment replication stage fields
    private long segmentTailSequenceNumber;
    private long segmentTailNextPosition;

    public VolumeInspectReplicationOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        String value = StandardCharsets.UTF_8.decode(bytes).toString();
        handleValue(value);
    }

    @Override
    public void setSingle(ByteBuffer bytes) {
        String value = StandardCharsets.UTF_8.decode(bytes).toString();
        handleValue(value);
    }

    @Override
    public void set(long integer) {
        handleValue(integer);
    }

    private void handleValue(Object value) {
        if (pendingKey == null) {
            pendingKey = value.toString();
        } else {
            String context = contextStack.peek();
            if (context == null) {
                // Root level
                switch (pendingKey) {
                    case "stage" -> stage = (String) value;
                    case "status" -> status = (String) value;
                    case "error_message" -> errorMessage = (String) value;
                }
            } else {
                switch (context) {
                    case "cursor" -> {
                        switch (pendingKey) {
                            case "segment_id" -> cursorSegmentId = (Long) value;
                            case "position" -> cursorPosition = (Long) value;
                        }
                    }
                    case "cdc_stage" -> {
                        switch (pendingKey) {
                            case "sequence_number" -> cdcSequenceNumber = (Long) value;
                            case "position" -> cdcPosition = (Long) value;
                        }
                    }
                    case "segment_replication_stage" -> {
                        switch (pendingKey) {
                            case "tail_sequence_number" -> segmentTailSequenceNumber = (Long) value;
                            case "tail_next_position" -> segmentTailNextPosition = (Long) value;
                        }
                    }
                }
            }
            pendingKey = null;
        }
    }

    @Override
    public void multi(int count) {
        if (pendingKey != null) {
            contextStack.push(pendingKey);
            pendingKey = null;
        }
    }

    @Override
    public void complete(int depth) {
        if (depth == 1 && !contextStack.isEmpty()) {
            contextStack.pop();
        }
        if (depth == 0) {
            VolumeInspectReplicationResponse.Cursor cursor =
                    new VolumeInspectReplicationResponse.Cursor(cursorSegmentId, cursorPosition);
            VolumeInspectReplicationResponse.CDCStage cdcStage =
                    new VolumeInspectReplicationResponse.CDCStage(cdcSequenceNumber, cdcPosition);
            VolumeInspectReplicationResponse.SegmentReplicationStage segmentReplicationStage =
                    new VolumeInspectReplicationResponse.SegmentReplicationStage(segmentTailSequenceNumber, segmentTailNextPosition);

            output = new VolumeInspectReplicationResponse(stage, cursor, status, errorMessage, cdcStage, segmentReplicationStage);
        }
    }
}
