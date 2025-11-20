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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

import static com.kronotop.volume.Subspaces.SEGMENT_LOG_SUBSPACE;

public class SegmentLog {
    private static final byte TRIGGER = 0x01;
    private static final byte[] ONE = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private static final byte[] NULL_BYTES = new byte[]{};

    // Volume root subspace
    private final DirectorySubspace subspace;

    public SegmentLog(DirectorySubspace subspace) {
        this.subspace = subspace;
    }

    private void triggerWatchers(Transaction tr, long segmentId) {
        Tuple tuple = Tuple.from(SEGMENT_LOG_SUBSPACE, TRIGGER, segmentId);
        byte[] key = subspace.pack(tuple);
        tr.mutate(MutationType.ADD, key, ONE);
    }

    public void appendOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, int userVersion) {
        Tuple tuple = Tuple.from(
                SEGMENT_LOG_SUBSPACE,
                metadata.segmentId(),
                OperationKind.APPEND.getValue(),
                metadata.position(),
                metadata.length(),
                prefix.asLong(),
                Versionstamp.incomplete(userVersion)
        );
        byte[] key = subspace.packWithVersionstamp(tuple);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, NULL_BYTES);
        triggerWatchers(tr, metadata.segmentId());
    }

    public void appendOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, Versionstamp versionstamp) {
        // Use this updating an existing entry.
        Tuple tuple = Tuple.from(
                SEGMENT_LOG_SUBSPACE,
                metadata.segmentId(),
                OperationKind.APPEND.getValue(),
                metadata.position(),
                metadata.length(),
                prefix.asLong(),
                versionstamp
        );
        byte[] key = subspace.pack(tuple);
        tr.set(key, NULL_BYTES);
        triggerWatchers(tr, metadata.segmentId());
    }

    public void deleteOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, Versionstamp versionstamp) {
        Tuple tuple = Tuple.from(
                SEGMENT_LOG_SUBSPACE,
                metadata.segmentId(),
                OperationKind.DELETE.getValue(),
                metadata.position(),
                metadata.length(),
                prefix.asLong(),
                versionstamp
        );
        byte[] key = subspace.pack(tuple);
        tr.set(key, NULL_BYTES);
        triggerWatchers(tr, metadata.segmentId());
    }
}
