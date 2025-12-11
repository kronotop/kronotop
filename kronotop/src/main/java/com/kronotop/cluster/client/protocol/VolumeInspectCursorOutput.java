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

/**
 * Lettuce output decoder for VOLUME.INSPECT CURSOR command response.
 */
public class VolumeInspectCursorOutput<K, V> extends CommandOutput<K, V, VolumeInspectCursorResponse> {

    private String pendingKey;
    private long activeSegmentId;
    private String versionstamp;
    private long nextPosition;
    private long sequenceNumber;

    public VolumeInspectCursorOutput(RedisCodec<K, V> codec) {
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
            switch (pendingKey) {
                case "active_segment_id" -> activeSegmentId = (Long) value;
                case "versionstamp" -> versionstamp = (String) value;
                case "next_position" -> nextPosition = (Long) value;
                case "sequence_number" -> sequenceNumber = (Long) value;
            }
            pendingKey = null;
        }
    }

    @Override
    public void complete(int depth) {
        if (depth == 0) {
            output = new VolumeInspectCursorResponse(activeSegmentId, versionstamp, nextPosition, sequenceNumber);
        }
    }
}
