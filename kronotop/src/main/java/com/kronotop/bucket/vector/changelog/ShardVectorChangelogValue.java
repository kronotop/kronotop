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

package com.kronotop.bucket.vector.changelog;

import com.kronotop.bucket.index.MutationLogMarker;

import java.nio.ByteBuffer;

/**
 * Value stored in a per-shard vector changelog entry. Unlike the per-index mutation log,
 * the value is objectId-only: it carries no vector payload. Replay re-reads the vector
 * from the index ENTRIES subspace.
 *
 * <p>Wire format (fixed 29 bytes): {@code [8B timestamp][8B indexId][1B marker][12B objectId]}
 *
 * @param timestamp physical wall-clock millis (context.now()) at append time, used for retention
 * @param indexId   the vector index identifier this mutation belongs to
 * @param marker    the mutation type (INSERT, UPDATE, DELETE)
 * @param objectId  the 12-byte document ObjectId
 */
public record ShardVectorChangelogValue(long timestamp, long indexId,
                                        MutationLogMarker marker, byte[] objectId) {

    public static final int OBJECT_ID_LENGTH = 12;
    public static final int ENCODED_LENGTH = Long.BYTES + Long.BYTES + 1 + OBJECT_ID_LENGTH;

    /**
     * Encodes a changelog value to its wire format.
     */
    public static byte[] encode(long timestamp, long indexId, MutationLogMarker marker, byte[] objectId) {
        if (objectId.length != OBJECT_ID_LENGTH) {
            throw new IllegalArgumentException("objectId must be " + OBJECT_ID_LENGTH + " bytes");
        }
        ByteBuffer buf = ByteBuffer.allocate(ENCODED_LENGTH);
        buf.putLong(timestamp);
        buf.putLong(indexId);
        buf.put(marker.getValue());
        buf.put(objectId);
        return buf.array();
    }

    /**
     * Decodes a changelog value from its wire format.
     */
    public static ShardVectorChangelogValue decode(byte[] value) {
        ByteBuffer buf = ByteBuffer.wrap(value);
        long timestamp = buf.getLong();
        long indexId = buf.getLong();
        MutationLogMarker marker = MutationLogMarker.fromValue(buf.get());
        byte[] objectId = new byte[OBJECT_ID_LENGTH];
        buf.get(objectId);
        return new ShardVectorChangelogValue(timestamp, indexId, marker, objectId);
    }
}
