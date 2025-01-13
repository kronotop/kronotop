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

public enum OperationKind {
    APPEND((byte) 0x01),
    DELETE((byte) 0x02),
    VACUUM((byte) 0x03);

    private final byte value;

    OperationKind(byte value) {
        this.value = value;
    }

    public static OperationKind valueOf(byte value) {
        return switch (value) {
            case 0x01 -> APPEND;
            case 0x02 -> DELETE;
            case 0x03 -> VACUUM;
            default -> throw new IllegalArgumentException();
        };
    }

    public byte getValue() {
        return value;
    }
}
