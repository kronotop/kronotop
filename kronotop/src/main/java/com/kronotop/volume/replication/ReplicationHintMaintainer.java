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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeSession;

import java.nio.ByteBuffer;

import static com.kronotop.volume.Subspaces.REPLICATION_HINT_SUBSPACE;

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

        ByteBuffer buffer = ByteBuffer.allocate(1 + metadata.prefix().length + Long.BYTES);
        buffer.put(kind.getValue());
        buffer.putLong(metadata.length());
        buffer.put(metadata.prefix());

        for (long indexId : hint.indexIds(index)) {
            byte[] key = subspace.pack(Tuple.from(REPLICATION_HINT_SUBSPACE, metadata.segmentId(), metadata.position(), indexId));
            session.transaction().set(key, buffer.array());
        }
    }

    /**
     * Decodes one hint key/value pair written by {@link #setHintIfMarked}.
     */
    public static ReplicationHintEntry decode(DirectorySubspace subspace, byte[] key, byte[] value) {
        Tuple tuple = subspace.unpack(key);
        ByteBuffer buffer = ByteBuffer.wrap(value);
        ReplicationHintKind kind = ReplicationHintKind.valueOf(buffer.get());
        long length = buffer.getLong();
        byte[] prefix = new byte[buffer.remaining()];
        buffer.get(prefix);
        return new ReplicationHintEntry(kind, tuple.getLong(1), tuple.getLong(2), length, prefix, tuple.getLong(3));
    }
}
