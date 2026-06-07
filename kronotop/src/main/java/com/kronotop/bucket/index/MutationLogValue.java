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

package com.kronotop.bucket.index;

import java.nio.ByteBuffer;

/**
 * Value stored in a mutation log entry. Combines a marker, document ObjectId, and an optional
 * VectorIndexValue payload (present for INSERT/UPDATE, absent for DELETE).
 *
 * <p>Wire format: {@code [1B marker][12B objectId][VectorIndexValue bytes (optional)]}
 *
 * @param marker        the mutation type (INSERT, UPDATE, DELETE)
 * @param objectIdBytes the 12-byte document ObjectId
 * @param vectorPayload the optional VectorIndexValue (null for DELETE)
 */
public record MutationLogValue(MutationLogMarker marker, byte[] objectIdBytes, VectorIndexValue vectorPayload) {

    private static final int OBJECT_ID_LENGTH = 12;

    /**
     * Encodes a mutation log value for INSERT or UPDATE operations.
     */
    public static byte[] encode(MutationLogMarker marker, byte[] objectIdBytes, byte[] encodedIndexEntry, float[] vector) {
        byte[] vectorPayload = VectorIndexValue.encode(encodedIndexEntry, vector);
        ByteBuffer buf = ByteBuffer.allocate(1 + OBJECT_ID_LENGTH + vectorPayload.length);
        buf.put(marker.getValue());
        buf.put(objectIdBytes);
        buf.put(vectorPayload);
        return buf.array();
    }

    /**
     * Encodes a mutation log value for DELETE operations (no vector payload).
     */
    public static byte[] encode(MutationLogMarker marker, byte[] objectIdBytes) {
        ByteBuffer buf = ByteBuffer.allocate(1 + OBJECT_ID_LENGTH);
        buf.put(marker.getValue());
        buf.put(objectIdBytes);
        return buf.array();
    }

    /**
     * Decodes a mutation log value from its wire format.
     */
    public static MutationLogValue decode(byte[] value) {
        ByteBuffer buf = ByteBuffer.wrap(value);
        MutationLogMarker marker = MutationLogMarker.fromValue(buf.get());
        byte[] objectIdBytes = new byte[OBJECT_ID_LENGTH];
        buf.get(objectIdBytes);

        VectorIndexValue vectorPayload = null;
        if (buf.hasRemaining()) {
            byte[] remaining = new byte[buf.remaining()];
            buf.get(remaining);
            vectorPayload = VectorIndexValue.decode(remaining);
        }

        return new MutationLogValue(marker, objectIdBytes, vectorPayload);
    }
}
