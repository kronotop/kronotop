/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeSession;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.volume.Subspaces.REPLICATION_HINT_SUBSPACE;
import static com.kronotop.volume.Subspaces.SEGMENT_POSITION_SUBSPACE;

public class ReplicationHintMaintainer {

    /**
     * Writes one hint key per vector index id when the session carries a hint and the entry at {@code index} is marked.
     */
    public static void setHintIfMarked(
            VolumeSession session,
            int index,
            DirectorySubspace subspace,
            ReplicationHintKind kind,
            EntryMetadata metadata
    ) {
        ReplicationHint hint = session.getReplicationHint();
        if (hint == null || !hint.hasVector(index)) {
            return;
        }

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + metadata.prefix().length);
        buffer.putLong(metadata.length());
        buffer.putLong(metadata.handle());
        buffer.put(metadata.prefix());

        for (long indexId : hint.indexIds(index)) {
            Tuple key = Tuple.from(REPLICATION_HINT_SUBSPACE, metadata.segmentId(), metadata.position(), indexId, kind.getValue());
            session.transaction().set(subspace.pack(key), buffer.array());
        }
    }

    /**
     * Decodes one hint key/value pair written by {@link #setHintIfMarked}.
     */
    public static ReplicationHintEntry decode(DirectorySubspace subspace, byte[] key, byte[] value) {
        Tuple tuple = subspace.unpack(key);
        ByteBuffer buffer = ByteBuffer.wrap(value);
        long length = buffer.getLong();
        long handle = buffer.getLong();
        byte[] prefix = new byte[buffer.remaining()];
        buffer.get(prefix);
        EntryMetadata metadata = new EntryMetadata(tuple.getLong(1), prefix, tuple.getLong(2), length, handle);
        return new ReplicationHintEntry(
                ReplicationHintKind.valueOf((byte) tuple.getLong(4)),
                tuple.getLong(3),
                metadata
        );
    }

    /**
     * Scans the hints of a segment in {@code [from, to)} and returns the ones whose entry is fully
     * inside the range. The scan stops at the first entry that ends after {@code to}; the returned
     * cursor is its position, so the next scan picks it up again. Hint keys are ordered by position,
     * and every key of one entry shares that position.
     */
    public static ReplicationHintScan scan(Transaction tr, DirectorySubspace subspace, long segmentId, long from, long to) {
        byte[] begin = subspace.pack(Tuple.from(REPLICATION_HINT_SUBSPACE, segmentId, from));
        byte[] end = subspace.pack(Tuple.from(REPLICATION_HINT_SUBSPACE, segmentId, to));
        List<ReplicationHintEntry> hints = new ArrayList<>();
        for (KeyValue keyValue : tr.getRange(new Range(begin, end))) {
            ReplicationHintEntry hint = decode(subspace, keyValue.getKey(), keyValue.getValue());
            if (hint.metadata().position() + hint.metadata().length() > to) {
                return new ReplicationHintScan(hints, hint.metadata().position());
            }
            hints.add(hint);
        }
        return new ReplicationHintScan(hints, to);
    }

    /**
     * Returns the position where hint processing starts when segment replication resumes at
     * {@code position}. If an entry starts before {@code position} and ends after it, that entry
     * was incomplete when replication stopped, so its position is returned. Otherwise {@code position}.
     */
    public static long findHintCursor(Transaction tr, DirectorySubspace subspace, long segmentId, long position) {
        byte[] begin = subspace.pack(Tuple.from(SEGMENT_POSITION_SUBSPACE, segmentId));
        byte[] end = subspace.pack(Tuple.from(SEGMENT_POSITION_SUBSPACE, segmentId, position + 1));
        List<KeyValue> last = tr.getRange(new Range(begin, end), 1, true).asList().join();
        if (last.isEmpty()) {
            return position;
        }
        Tuple key = subspace.unpack(last.getFirst().getKey());
        long entryPosition = key.getLong(2);
        long length = key.getLong(3);
        if (entryPosition + length > position) {
            return entryPosition;
        }
        return position;
    }

    /**
     * Reads the versionstamps of the entries that start in {@code [from, to)} of a segment, keyed by position.
     */
    public static Map<Long, Versionstamp> readVersionstamps(Transaction tr, DirectorySubspace subspace, long segmentId, long from, long to) {
        byte[] begin = subspace.pack(Tuple.from(SEGMENT_POSITION_SUBSPACE, segmentId, from));
        byte[] end = subspace.pack(Tuple.from(SEGMENT_POSITION_SUBSPACE, segmentId, to));
        Map<Long, Versionstamp> versionstamps = new HashMap<>();
        for (KeyValue keyValue : tr.getRange(new Range(begin, end))) {
            long position = subspace.unpack(keyValue.getKey()).getLong(2);
            versionstamps.put(position, VersionstampUtil.decodeVersionstampedValue(keyValue.getValue()));
        }
        return versionstamps;
    }
}
