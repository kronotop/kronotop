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

package com.kronotop.volume;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

import javax.annotation.Nonnull;

import static com.kronotop.volume.Subspaces.*;

public class VolumeSubspace {
    private final DirectorySubspace subspace;

    VolumeSubspace(@Nonnull DirectorySubspace subspace) {
        this.subspace = subspace;
    }

    byte[] packEntryKeyPrefix(Prefix prefix) {
        Tuple tuple = Tuple.from(ENTRY_SUBSPACE, prefix.asBytes());
        return subspace.pack(tuple);
    }

    byte[] packEntryKey(Prefix prefix, Versionstamp key) {
        Tuple tuple = Tuple.from(ENTRY_SUBSPACE, prefix.asBytes(), key);
        return subspace.pack(tuple);
    }

    byte[] packEntryKeyWithVersionstamp(Prefix prefix, int version) {
        Tuple tuple = Tuple.from(ENTRY_SUBSPACE, prefix.asBytes(), Versionstamp.incomplete(version));
        return subspace.packWithVersionstamp(tuple);
    }

    byte[] packEntryMetadataKey(byte[] data) {
        return subspace.pack(Tuple.from(ENTRY_METADATA_SUBSPACE, data));
    }

    byte[] packSegmentCardinalityKey(long segmentId, Prefix prefix) {
        return subspace.pack(Tuple.from(SEGMENT_CARDINALITY_SUBSPACE, segmentId, prefix.asBytes()));
    }

    byte[] packSegmentUsedBytesKey(long segmentId, Prefix prefix) {
        return subspace.pack(Tuple.from(SEGMENT_USED_BYTES_SUBSPACE, segmentId, prefix.asBytes()));
    }
}
