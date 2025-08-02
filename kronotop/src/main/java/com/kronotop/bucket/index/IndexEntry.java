// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.index;

import com.kronotop.volume.EntryMetadata;

import java.util.Arrays;

public record IndexEntry(int shardId, byte[] entryMetadata) {
    public static int SHARD_ID_SIZE = 4;

    public static IndexEntry decode(byte[] encoded) {
        int shardId = ((encoded[0] & 0xFF) << 24) |
                ((encoded[1] & 0xFF) << 16) |
                ((encoded[2] & 0xFF) << 8) |
                (encoded[3] & 0xFF);
        byte[] metadata = Arrays.copyOfRange(encoded, SHARD_ID_SIZE, encoded.length);
        return new IndexEntry(shardId, metadata);
    }

    // 4 bytes for shardId (int), plus entry metadata length
    public byte[] encode() {
        byte[] result = new byte[SHARD_ID_SIZE + EntryMetadata.SIZE];
        result[0] = (byte) ((shardId >>> 24) & 0xFF);
        result[1] = (byte) ((shardId >>> 16) & 0xFF);
        result[2] = (byte) ((shardId >>> 8) & 0xFF);
        result[3] = (byte) (shardId & 0xFF);
        System.arraycopy(entryMetadata, 0, result, SHARD_ID_SIZE, EntryMetadata.SIZE);
        return result;
    }
}
