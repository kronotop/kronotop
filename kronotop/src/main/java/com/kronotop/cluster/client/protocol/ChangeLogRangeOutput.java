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
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Lettuce output decoder for CHANGELOGRANGE command responses.
 * <p>
 * Decodes a nested RESP3 structure into strongly-typed DTOs:
 * <pre>
 * Array [
 *     Map {
 *         "versionstamp" -> string
 *         "kind" -> string
 *         "prefix" -> integer
 *         "before" -> Map { sequence_number, segment_id, position, length }  // optional
 *         "after" -> Map { sequence_number, segment_id, position, length }   // optional
 *     },
 *     ...
 * ]
 * </pre>
 */
public class ChangeLogRangeOutput<K, V> extends CommandOutput<K, V, List<ChangeLogEntryResponse>> {

    private final Deque<MapBuilder> stack = new ArrayDeque<>();
    private String pendingKey;
    private int depth;

    public ChangeLogRangeOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<>());
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
        if (depth == 0 || stack.isEmpty()) {
            return;
        }

        MapBuilder current = stack.peek();
        if (pendingKey == null) {
            pendingKey = value.toString();
        } else {
            current.put(pendingKey, value);
            pendingKey = null;
        }
    }

    @Override
    public void multiArray(int count) {
        depth++;
    }

    @Override
    public void multiMap(int count) {
        depth++;

        MapBuilder builder;
        if (stack.isEmpty()) {
            builder = new EntryBuilder();
        } else {
            builder = new CoordinateBuilder();
            // Link to parent
            MapBuilder parent = stack.peek();
            if (parent instanceof EntryBuilder entryBuilder && pendingKey != null) {
                entryBuilder.setNestedCoordinate(pendingKey, (CoordinateBuilder) builder);
                pendingKey = null;
            }
        }
        stack.push(builder);
    }

    @Override
    public void complete(int depth) {
        if (this.depth > depth) {
            this.depth--;

            if (!stack.isEmpty()) {
                MapBuilder completed = stack.pop();

                if (stack.isEmpty() && completed instanceof EntryBuilder entryBuilder) {
                    output.add(entryBuilder.build());
                }
            }
        }
    }

    private interface MapBuilder {
        void put(String key, Object value);
    }

    private static class EntryBuilder implements MapBuilder {
        private String versionstamp;
        private String kind;
        private long prefix;
        private CoordinateBuilder before;
        private CoordinateBuilder after;

        @Override
        public void put(String key, Object value) {
            switch (key) {
                case "versionstamp" -> versionstamp = (String) value;
                case "kind" -> kind = (String) value;
                case "prefix" -> prefix = (Long) value;
            }
        }

        void setNestedCoordinate(String key, CoordinateBuilder builder) {
            if ("before".equals(key)) {
                before = builder;
            } else if ("after".equals(key)) {
                after = builder;
            }
        }

        ChangeLogEntryResponse build() {
            return new ChangeLogEntryResponse(
                    versionstamp,
                    kind,
                    prefix,
                    before != null ? before.build() : null,
                    after != null ? after.build() : null
            );
        }
    }

    private static class CoordinateBuilder implements MapBuilder {
        private long sequenceNumber;
        private long segmentId;
        private long position;
        private long length;

        @Override
        public void put(String key, Object value) {
            long longValue = (Long) value;
            switch (key) {
                case "sequence_number" -> sequenceNumber = longValue;
                case "segment_id" -> segmentId = longValue;
                case "position" -> position = longValue;
                case "length" -> length = longValue;
            }
        }

        ChangeLogCoordinateResponse build() {
            return new ChangeLogCoordinateResponse(sequenceNumber, segmentId, position, length);
        }
    }
}
