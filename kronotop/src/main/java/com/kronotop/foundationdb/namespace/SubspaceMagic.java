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


package com.kronotop.foundationdb.namespace;

/**
 * Represents constants used to define specific subspaces or structures
 * within a FoundationDB hierarchy. Each constant in this enum is associated
 * with a unique byte value that acts as an identifier for the corresponding subspace.
 */
public enum SubspaceMagic {
    ZMAP((byte) 0x01),
    BUCKET((byte) 0x02),
    BUCKET_PREFIX((byte) 0x03),
    BUCKET_INDEX((byte) 0x04);

    public final byte value;

    SubspaceMagic(byte kind) {
        value = kind;
    }

    public byte getValue() {
        return value;
    }
}
