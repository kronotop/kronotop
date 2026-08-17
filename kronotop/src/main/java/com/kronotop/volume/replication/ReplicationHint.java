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

package com.kronotop.volume.replication;

import java.nio.ByteBuffer;

public record ReplicationHint(HintType type, long segmentId, long position) {
    public static final int ENCODED_SIZE = 17;

    /**
     * Decodes a replication hint from a fixed-size byte array.
     *
     * @param data 17-byte array containing the encoded hint
     * @return decoded ReplicationHint instance
     * @throws IllegalArgumentException if the array size is invalid
     */
    public static ReplicationHint decode(byte[] data) {
        if (data.length != ENCODED_SIZE) {
            throw new IllegalArgumentException("Invalid replication hint size: " + data.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        HintType type = HintType.fromValue(buffer.get());
        long segmentId = buffer.getLong();
        long position = buffer.getLong();
        return new ReplicationHint(type, segmentId, position);
    }

    /**
     * Extracts the hint type without decoding the whole hint.
     *
     * @param data 17-byte array containing the encoded hint
     * @return the hint type
     * @throws IllegalArgumentException if the array size is invalid
     */
    public static HintType extractType(byte[] data) {
        if (data.length != ENCODED_SIZE) {
            throw new IllegalArgumentException("Invalid replication hint size: " + data.length);
        }
        return HintType.fromValue(data[0]);
    }

    /**
     * Encodes the replication hint into a fixed-size byte array.
     *
     * @return 17-byte array
     */
    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(ENCODED_SIZE);
        buffer.put(type.getValue());
        buffer.putLong(segmentId);
        buffer.putLong(position);
        return buffer.array();
    }
}
