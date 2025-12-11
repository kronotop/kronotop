/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;

import java.util.Arrays;
import java.util.List;

import static com.kronotop.volume.Subspaces.ENTRY_METADATA_SUBSPACE;

/**
 * Utility class for segment-related operations in the volume subsystem.
 * Provides helper methods for constructing FoundationDB key prefixes and finding segment positions.
 */
public class SegmentUtil {

    public static long findActiveSegmentId(Transaction tr, DirectorySubspace subspace) {
        VolumeMetadata metadata = VolumeMetadata.load(tr, subspace);
        if (metadata.getSegments().isEmpty()) {
            return 0;
        }
        return metadata.getSegments().getLast();
    }

    /**
     * Constructs a FoundationDB key prefix for entries in a segment filtered by volume prefix.
     *
     * @param subspace  the directory subspace for the volume
     * @param segmentId the segment identifier
     * @param prefix    the volume prefix to filter entries
     * @return the packed key prefix for querying entries in the segment with the specified volume prefix
     */
    static byte[] prefixOfVolumePrefix(DirectorySubspace subspace, long segmentId, Prefix prefix) {
        byte[] begin = subspace.pack(Tuple.from(ENTRY_METADATA_SUBSPACE, Tuple.from(segmentId, prefix.asBytes()).pack()));
        return Arrays.copyOf(begin, begin.length - 1);
    }

    /**
     * Constructs a FoundationDB key prefix for all entries in a segment.
     *
     * @param subspace  the directory subspace for the volume
     * @param segmentId the segment identifier
     * @return the packed key prefix for querying all entries in the segment
     */
    static byte[] segmentPrefix(DirectorySubspace subspace, long segmentId) {
        byte[] prefix = subspace.pack(Tuple.from(ENTRY_METADATA_SUBSPACE, Tuple.from(segmentId).pack()));
        return Arrays.copyOf(prefix, prefix.length - 1);
    }

    /**
     * Finds the next available position in a segment by locating the last entry.
     * This method performs a reverse range scan to find the highest position used in the segment.
     *
     * @param context   the Kronotop context providing access to FoundationDB
     * @param subspace  the directory subspace for the volume
     * @param segmentId the segment identifier
     * @return the next available position (position + length of the last entry), or 0 if the segment is empty
     */
    public static long findNextPosition(Context context, DirectorySubspace subspace, long segmentId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentTailPointer link = locateTailPointer(tr, subspace, segmentId);
            return link.nextPosition();
        }
    }

    /**
     * Locates the tail pointer of a segment by finding the last entry's metadata.
     *
     * @param tr        the transaction to use for the read operation
     * @param subspace  the directory subspace for the volume
     * @param segmentId the segment identifier
     * @return a {@link SegmentTailPointer} containing the versionstamp and next available position,
     * or a pointer with null versionstamp and position 0 if the segment is empty
     */
    public static SegmentTailPointer locateTailPointer(Transaction tr, DirectorySubspace subspace, long segmentId) {
        byte[] segmentPrefix = segmentPrefix(subspace, segmentId);

        Range range = Range.startsWith(segmentPrefix);
        AsyncIterable<KeyValue> iterable = tr.getRange(range, 1, true);
        List<KeyValue> result = iterable.asList().join();
        if (result.isEmpty()) {
            // No entries found
            return new SegmentTailPointer(null, 0, 0);
        }

        byte[] key = result.getFirst().getKey();
        byte[] value = result.getFirst().getValue();

        byte[] data = (byte[]) subspace.unpack(key).get(1);
        EntryMetadata latest = EntryMetadata.decode(data);

        Versionstamp versionstamp = VolumeUtil.extractVersionstampFromValue(value);
        return new SegmentTailPointer(versionstamp, latest.position(), latest.length());
    }
}
