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

package com.kronotop.volume;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;

/**
 * Generates deterministic hash-based metadata identifiers for volume entries.
 * Uses MurmurHash3 to compute a 32-bit hash from volume ID, segment ID, and position.
 */
public class EntryMetadataIdGenerator {
    private static final int LENGTH = 20;
    private static final HashFunction MURMUR3_32_FIXED = Hashing.murmur3_32_fixed();

    /**
     * Generates a deterministic 32-bit hash from volume location coordinates.
     *
     * @param volumeId  the volume identifier
     * @param segmentId the segment identifier
     * @param position  the position within the segment
     * @return a 32-bit hash value
     */
    public static int generate(int volumeId, long segmentId, long position) {
        ByteBuffer buf = ByteBuffer.allocate(LENGTH);
        buf.putInt(volumeId);
        buf.putLong(segmentId);
        buf.putLong(position);
        buf.flip();
        return MURMUR3_32_FIXED.hashBytes(buf.array()).asInt();
    }
}
