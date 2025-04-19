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
 * Provides functionality to generate unique metadata IDs based on a segment ID and position.
 * <p>
 * This class uses the XXHash algorithm to create a fast and efficient hash value
 * for identifying metadata entries in a system. The resulting ID is computed
 * by combining the input parameters and applying a hashing procedure with a specified seed.
 */
public class EntryMetadataIdGenerator {
    private static final int SEED = 0x9747b28c;
    private static final int LENGTH = 16;

    /**
     * Generates a hash-based identifier using the provided segment ID and position.
     * This method combines the input parameters and applies a hashing procedure
     * utilizing the XXHash algorithm with a predefined seed value.
     *
     * @param segmentId the unique identifier of the segment
     * @param position  the position within the segment
     * @return a hash value representing the unique identifier for the given segment ID and position
     */
    public static int generate(long segmentId, long position) {
        ByteBuffer buf = ByteBuffer.allocate(LENGTH);
        try (StreamingXXHash32 hashFunc = XXHashFactory.fastestInstance().newStreamingHash32(SEED)) {
            buf.putLong(segmentId);
            buf.putLong(position);
            buf.flip();
            hashFunc.update(buf.array(), 0, LENGTH);
            return hashFunc.getValue();
        }
    }
}
