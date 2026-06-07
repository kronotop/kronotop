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

package com.kronotop.bucket;

public enum BucketMetadataMagic {
    HEADER((byte) 0x01),
    REMOVED((byte) 0x03),
    VERSION((byte) 0x04),
    SINGLE_FIELD_INDEX_DEFINITION((byte) 0x05),
    PREFIX_BINDING_KEY((byte) 0x06),
    INDEX_STATISTICS((byte) 0x07),
    CARDINALITY((byte) 0x08),
    HISTOGRAM((byte) 0x09),
    SHARDS((byte) 0x0A),
    COMPOUND_INDEX_DEFINITION((byte) 0x0B),
    VECTOR_INDEX_DEFINITION((byte) 0x0C),
    COLLATION((byte) 0x0D),
    NAMESPACE_BINDING_KEY((byte) 0x0E),
    BUCKET_NAME((byte) 0x0F);

    public final byte value;

    BucketMetadataMagic(byte kind) {
        value = kind;
    }

    public byte getValue() {
        return value;
    }

    public long getLong() {
        return value & 0xFFL;
    }
}
