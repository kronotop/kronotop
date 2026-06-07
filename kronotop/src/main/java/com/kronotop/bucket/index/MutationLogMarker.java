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

/**
 * Markers for mutation log entries that record vector index mutations for crash recovery.
 */
public enum MutationLogMarker {
    INSERT((byte) 0x01),
    UPDATE((byte) 0x02),
    DELETE((byte) 0x03);

    private final byte value;

    MutationLogMarker(byte value) {
        this.value = value;
    }

    public static MutationLogMarker fromValue(byte value) {
        return switch (value) {
            case 0x01 -> INSERT;
            case 0x02 -> UPDATE;
            case 0x03 -> DELETE;
            default -> throw new IllegalArgumentException("Unknown MutationLogMarker value: " + value);
        };
    }

    public byte getValue() {
        return value;
    }
}
