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
        EntryMetadata metadata = VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test");
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