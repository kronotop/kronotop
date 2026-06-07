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
 * Value stored in a vector index entry. Combines an IndexEntry with the raw vector data.
 *
 * <p>Wire format: {@code [4B indexEntryLen (BE int)][encodedIndexEntry (variable)][float[] (dimensions * 4B, IEEE 754 BE)]}
 *
 * @param indexEntry the document's index entry (shard + metadata)
 * @param vector     the document's vector data
 */
public record VectorIndexValue(IndexEntry indexEntry, float[] vector) {

    /**
     * Encodes an IndexEntry and vector into the wire format.
     *
     * @param encodedIndexEntry pre-encoded IndexEntry bytes
     * @param vector            the vector data
     * @return encoded byte array
     */
    public static byte[] encode(byte[] encodedIndexEntry, float[] vector) {
        int totalLen = 4 + encodedIndexEntry.length + vector.length * 4;
        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.putInt(encodedIndexEntry.length);
        buf.put(encodedIndexEntry);
        for (float v : vector) {
            buf.putFloat(v);
        }
        return buf.array();
    }

    /**
     * Decodes a VectorIndexValue from its wire format.
     *
     * @param value the encoded bytes
     * @return decoded VectorIndexValue
     */
    public static VectorIndexValue decode(byte[] value) {
        ByteBuffer buf = ByteBuffer.wrap(value);
        int indexEntryLen = buf.getInt();
        byte[] encodedIndexEntry = new byte[indexEntryLen];
        buf.get(encodedIndexEntry);
        int remaining = buf.remaining() / 4;
        float[] vector = new float[remaining];
        for (int i = 0; i < remaining; i++) {
            vector[i] = buf.getFloat();
        }
        return new VectorIndexValue(IndexEntry.decode(encodedIndexEntry), vector);
    }
}
