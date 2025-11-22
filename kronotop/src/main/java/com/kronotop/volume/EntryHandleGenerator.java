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

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class EntryHandleGenerator {
    private static final HashFunction MURMUR3_128_FIXED = Hashing.murmur3_128();

    /**
     * Generates a 64-bit deterministic handle from the physical
     * storage coordinates of an entry: {@code (volumeId, segmentId, position)}.
     *
     * <p>This handle is <b>not</b> a logical identifier and must not be confused
     * with FDB-assigned Versionstamps or any form of globally unique ID.
     * It is simply a compact, hash-derived reference used internally for
     * indexing, bitmap operations, and other high-fanout lookup structures.
     *
     * <p>The method computes a 128-bit Murmur3 hash over the three coordinates
     * and then mixes the two 64-bit halves into a single 64-bit value:
     *
     * <pre>
     *   hash128 = Murmur3_128(volumeId, segmentId, position)
     *   handle  = hi64(hash128) XOR ROTL64(lo64(hash128), 32)
     * </pre>
     *
     * <p><b>Why 128-bit → 64-bit reduction?</b><br>
     * - The storage coordinates already form a high-entropy domain.
     * - Murmur3_128 provides excellent avalanche and uniform distribution.
     * - Reducing to 64 bits is a practical trade-off for fast bitmap
     * and in-memory indexing without the collision profile of 32-bit hashes.
     *
     * <p><b>Collision characteristics:</b><br>
     * - The final handle lives in a 64-bit space (~1.8e19 values).<br>
     * - With uniform Murmur3_128 output, the probability of a collision at
     * realistic entry counts is negligible (birthday bound). Even at
     * 1 billion distinct entries, the collision probability remains
     * astronomically small (~1e-10).<br>
     * - The XOR+rotate mixing step prevents the two 64-bit halves of the
     * 128-bit hash from contributing unevenly.
     *
     * <p>The function is deterministic and idempotent: the same physical
     * coordinates always produce the same handle. This aids debugging and
     * avoids the instability of timestamp- or randomness-based schemes.
     *
     * @return a 64-bit non-unique but practically collision-free handle
     * for the given entry location.
     */
    public static long generate(long volumeId, long segmentId, long position) {
        HashCode code = MURMUR3_128_FIXED.
                newHasher(24).
                putLong(volumeId).
                putLong(segmentId).
                putLong(position).
                hash();

        byte[] bytes = code.asBytes(); // 16 bytes
        long high = 0;
        for (int i = 0; i < 8; i++) {
            high = (high << 8) | (bytes[i] & 0xff);
        }

        long low = 0;
        for (int i = 8; i < 16; i++) {
            low = (low << 8) | (bytes[i] & 0xff);
        }

        return high ^ Long.rotateLeft(low, 32);
    }
}
