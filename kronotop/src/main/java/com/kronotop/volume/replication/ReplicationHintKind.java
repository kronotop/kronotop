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

package com.kronotop.volume.replication;

public enum ReplicationHintKind {
    VECTOR_NODE_ADD((byte) 0x01),
    VECTOR_NODE_DELETE((byte) 0x02);

    private final byte value;

    ReplicationHintKind(byte value) {
        this.value = value;
    }

    public static ReplicationHintKind valueOf(byte value) {
        return switch (value) {
            case 0x01 -> VECTOR_NODE_ADD;
            case 0x02 -> VECTOR_NODE_DELETE;
            default -> throw new IllegalArgumentException("Unknown replication hint kind: " + value);
        };
    }

    public byte getValue() {
        return value;
    }
}
