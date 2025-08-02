// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.index;

import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeTestUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;


class IndexEntryTest {
    @Test
    void shouldEncodeDecode() {
        EntryMetadata metadata = VolumeTestUtil.generateEntryMetadata(1, 0, 1, "test");
        // Invoke method on test
        ByteBuffer result = metadata.encode();

        byte[] encodedMetadata = result.array();

        int shardId = 10;
        IndexEntry indexEntry = new IndexEntry(shardId, encodedMetadata);

        byte[] encodedIndexEntry = indexEntry.encode();
        assertEquals(IndexEntry.SHARD_ID_SIZE + EntryMetadata.SIZE, encodedIndexEntry.length);

        IndexEntry decoded = IndexEntry.decode(encodedIndexEntry);
        assertEquals(shardId, decoded.shardId());
        assertArrayEquals(encodedMetadata, decoded.entryMetadata());
    }
}