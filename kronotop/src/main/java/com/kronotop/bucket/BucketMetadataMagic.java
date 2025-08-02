// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

public enum BucketMetadataMagic {
    HEADER((byte) 0x01),
    VERSION((byte) 0x02),
    INDEX_DEFINITION((byte) 0x03),
    VOLUME_PREFIX((byte) 0x04),
    INDEX_STATISTICS((byte) 0x05),
    INDEX_CARDINALITY((byte) 0x06);

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
