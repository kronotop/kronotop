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

import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.nio.ByteBuffer;

/**
 * Provides functionality to generate unique metadata IDs using a hashing algorithm.
 * <p>
 * This class utilizes the XXHash algorithm, initialized with a predefined seed, to compute
 * a hash value based on the provided entity identifier, segment identifier, and position.
 * The generated hash serves as a unique representation of these combined input parameters.
 */
public class EntryMetadataIdGenerator {
    private static final int SEED = 0x9747b28c;
    private static final int LENGTH = 20;

    /**
     * Generates a unique hash value based on the provided parameters.
     * The hash is computed using the XXHash algorithm with a specified seed.
     *
     * @param volumeId        the identifier for the entity
     * @param segmentId the identifier for the segment
     * @param position  the position within the segment
     * @return a hash value as an integer representing the combined input parameters
     */
    public static int generate(int volumeId, long segmentId, long position) {
        ByteBuffer buf = ByteBuffer.allocate(LENGTH);
        try (StreamingXXHash32 hashFunc = XXHashFactory.fastestInstance().newStreamingHash32(SEED)) {
            buf.putInt(volumeId);
            buf.putLong(segmentId);
            buf.putLong(position);
            buf.flip();
            hashFunc.update(buf.array(), 0, LENGTH);
            return hashFunc.getValue();
        }
    }
}
