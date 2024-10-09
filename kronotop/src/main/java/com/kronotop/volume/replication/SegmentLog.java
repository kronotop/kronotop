/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.volume.replication;

// SegmentLog
// <segment-name><versionstamped-key><epoch> = <operation><position><length>

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

import static com.kronotop.volume.Subspaces.SEGMENT_LOG_CARDINALITY_SUBSPACE;
import static com.kronotop.volume.Subspaces.SEGMENT_LOG_SUBSPACE;

public class SegmentLog {
    private static final byte[] CARDINALITY_INCREASE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private final String segmentName;
    private final DirectorySubspace subspace;
    private final byte[] cardinalityKey;

    public SegmentLog(String segmentName, DirectorySubspace subspace) {
        this.segmentName = segmentName;
        this.subspace = subspace;

        Tuple key = Tuple.from(SEGMENT_LOG_CARDINALITY_SUBSPACE, segmentName);
        this.cardinalityKey = subspace.pack(key);
    }

    public void append(Transaction tr, int userVersion, SegmentLogValue value) {
        Tuple preKey = Tuple.from(
                SEGMENT_LOG_SUBSPACE,
                segmentName,
                Versionstamp.incomplete(userVersion),
                Instant.now().toEpochMilli()
        );
        byte[] key = subspace.packWithVersionstamp(preKey);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value.encode().array());
        tr.mutate(MutationType.ADD, cardinalityKey, CARDINALITY_INCREASE_DELTA);
    }

    public int getCardinality(Transaction tr) {
        byte[] data = tr.get(cardinalityKey).join();
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }
}
