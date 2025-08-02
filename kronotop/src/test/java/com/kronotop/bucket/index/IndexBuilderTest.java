// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeTestUtil;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexBuilderTest extends BaseStandaloneInstanceTest {
    final int SHARD_ID = 1;
    final String testBucketName = "test-bucket";

    byte[] getEncodedEntryMetadata() {
        return VolumeTestUtil.generateEntryMetadata(1, 0, 1, "test").encode().array();
    }

    private AppendedEntry[] getAppendedEntries() {
        AppendedEntry[] entries = new AppendedEntry[3];
        byte[] encodedEntryMetadata = getEncodedEntryMetadata();
        for (int index = 0; index < entries.length; index++) {
            AppendedEntry entry = new AppendedEntry(index, index, null, encodedEntryMetadata);
            entries[index] = entry;
        }
        return entries;
    }

    @Test
    void shouldSetDefaultIDIndex() {
        AppendedEntry[] entries = getAppendedEntries();
        assertDoesNotThrow(() -> {
            BucketMetadata metadata = getBucketMetadata(testBucketName);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuilder.setIDIndex(tr, SHARD_ID, metadata, entries);
                tr.commit().join();
            }
        });
    }

    @Test
    void shouldReadEntriesFromIdIndex() {
        AppendedEntry[] entries = getAppendedEntries();
        BucketMetadata metadata = getBucketMetadata(testBucketName);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.setIDIndex(tr, SHARD_ID, metadata, entries);
            tr.commit().join();
        }

        DirectorySubspace idIndexSubspace = metadata.indexes().getSubspace(DefaultIndexDefinition.ID);
        byte[] prefix = idIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        byte[] expectedEntryMetadata = getEncodedEntryMetadata();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexedEntries = tr.getRange(begin, end).asList().join();
            assertEquals(3, indexedEntries.size());

            for (int i = 0; i < indexedEntries.size(); i++) {
                KeyValue entry = indexedEntries.get(i);

                Tuple unpackedIndex = idIndexSubspace.unpack(entry.getKey());
                Versionstamp key = (Versionstamp) unpackedIndex.get(1);
                assertEquals(i, key.getUserVersion());

                IndexEntry indexEntry = IndexEntry.decode(entry.getValue());
                assertEquals(SHARD_ID, indexEntry.shardId());
                assertArrayEquals(expectedEntryMetadata, indexEntry.entryMetadata());
            }
        }
    }
}