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

package com.kronotop.internal.shapehash;

public class FNV1A {

    // FNV-1a hash constants for 64-bit
    protected static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    /**
     * FNV-1a hash mix function for integers.
     */
    protected static long mix(long hash, int value) {
        hash ^= value;
        hash *= FNV_PRIME;
        return hash;
    }

    /**
     * FNV-1a hash mix function for longs.
     */
    protected static long mix(long hash, long value) {
        hash ^= value;
        hash *= FNV_PRIME;
        return hash;
    }

    /**
     * FNV-1a hash mix function for strings.
     * Processes each character without creating intermediate objects.
     */
    protected static long mixString(long hash, String s) {
        for (int i = 0; i < s.length(); i++) {
            hash ^= s.charAt(i);
            hash *= FNV_PRIME;
        }
        return hash;
    }
}
