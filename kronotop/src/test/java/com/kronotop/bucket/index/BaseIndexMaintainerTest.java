/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeTestUtil;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BaseIndexMaintainerTest extends BaseStandaloneInstanceTest {
    protected final int SHARD_ID = 1;

    byte[] getEncodedEntryMetadata() {
        return VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test").encode();
    }

    protected AppendedEntry[] getAppendedEntries() {
        AppendedEntry[] entries = new AppendedEntry[3];
        byte[] encodedEntryMetadata = getEncodedEntryMetadata();
        for (int index = 0; index < entries.length; index++) {
            AppendedEntry entry = new AppendedEntry(index, index, null, encodedEntryMetadata);
            entries[index] = entry;
        }
        return entries;
    }

    /**
     * Verifies volume pointer entries in an index subspace.
     *
     * @return a map of ObjectId to userVersion for all volume pointers found
     */
    protected Map<ObjectId, Integer> getVolumePointers(DirectorySubspace indexSubspace) {
        Map<ObjectId, Integer> result = new HashMap<>();
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.VOLUME_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> volumePointers = tr.getRange(begin, end).asList().join();
            for (KeyValue kv : volumePointers) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                assertEquals((long) IndexSubspaceMagic.VOLUME_POINTER.getValue(), unpacked.get(0), "Magic value should be VOLUME_POINTER");

                byte[] versionstampBytes = unpacked.getVersionstamp(1).getBytes();
                Versionstamp versionstamp = Versionstamp.fromBytes(versionstampBytes);

                byte[] objectIdBytes = (byte[]) unpacked.get(2);
                ObjectId objectId = new ObjectId(objectIdBytes);

                assertArrayEquals(IndexMaintainer.NULL_VALUE, kv.getValue(), "Volume pointer value should be NULL_VALUE");
                result.put(objectId, versionstamp.getUserVersion());
            }
        }
        return result;
    }
}
