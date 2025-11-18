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

package com.kronotop.volume.segrep;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SegmentReplicationState {
    public static final byte SEGMENTS = 0x00;

    public static void setPosition(Transaction tr, DirectorySubspace subspace, long segmentId, long position) {
        Tuple tuple = Tuple.from(SEGMENTS, segmentId);
        byte[] key = subspace.pack(tuple.pack());
        byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(position).array();
        tr.set(key, value);
    }

    public static ReplicationCursor readCursor(Transaction tr, DirectorySubspace subspace) {
        Tuple tuple = Tuple.from(SEGMENTS);
        byte[] prefix = subspace.pack(tuple);
        Range range = Range.startsWith(prefix);
        for (KeyValue entry : tr.getRange(range, 1, true)) {
            Tuple key = Tuple.fromBytes(entry.getKey());
            long segmentId = key.getLong(1);
            long position = ByteBuffer.wrap(entry.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
            return new ReplicationCursor(segmentId, position);
        }
        return new ReplicationCursor(0, 0);
    }
}
