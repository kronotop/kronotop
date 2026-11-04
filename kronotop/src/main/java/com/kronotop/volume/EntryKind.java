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

package com.kronotop.volume;

/**
 * Kind of the entries stored under a Volume prefix. All entries of one prefix have the same kind. The kind decides
 * how the entries are evacuated and cleaned up, and what the pointer of the prefix refers to.
 */
public enum EntryKind {
    /**
     * Bucket document bodies. The pointer is the bucket UUID.
     */
    DOCUMENT((byte) 0x01),

    /**
     * Chunks of a flushed vector graph index. The pointer is the ObjectId of the flushed index.
     */
    VECTOR_INDEX((byte) 0x02);

    private final byte value;

    EntryKind(byte value) {
        this.value = value;
    }

    /**
     * Returns the kind for its wire value.
     *
     * @param value encoded kind
     * @return the kind
     * @throws IllegalArgumentException if the value is unknown
     */
    public static EntryKind valueOf(byte value) {
        return switch (value) {
            case 0x01 -> DOCUMENT;
            case 0x02 -> VECTOR_INDEX;
            default -> throw new IllegalArgumentException();
        };
    }

    public byte getValue() {
        return value;
    }
}
