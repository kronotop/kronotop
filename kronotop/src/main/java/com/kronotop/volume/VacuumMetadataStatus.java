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

package com.kronotop.volume;

public enum VacuumMetadataStatus {
    ANALYZE((byte) 0x01),
    EVACUATING((byte) 0x02),
    COMPLETED((byte) 0x03),
    STOPPED((byte) 0x04);

    private final byte value;

    VacuumMetadataStatus(byte value) {
        this.value = value;
    }

    public static VacuumMetadataStatus fromValue(byte value) {
        for (VacuumMetadataStatus status : values()) {
            if (status.value == value) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown vacuum metadata status: " + value);
    }

    public byte getValue() {
        return value;
    }
}
