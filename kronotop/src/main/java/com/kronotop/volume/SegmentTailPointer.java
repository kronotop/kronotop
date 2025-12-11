/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume;

import com.apple.foundationdb.tuple.Versionstamp;

/**
 * Represents the tail position of a segment, containing the last entry's versionstamp,
 * position, and length.
 *
 * @param versionstamp the versionstamp of the last entry, or null if the segment is empty
 * @param position     the byte offset of the last entry
 * @param length       the length of the last entry in bytes
 */
public record SegmentTailPointer(Versionstamp versionstamp, long position, long length) {
    /**
     * @return the next available byte offset for writing new entries (position + length)
     */
    public long nextPosition() {
        return position + length;
    }
}
