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
import com.kronotop.Context;

import static com.kronotop.volume.Subspaces.CHANGELOG_SUBSPACE;
import static com.kronotop.volume.Subspaces.MUTATION_TRIGGER;

public class ChangeLog {
    private static final byte[] INCREASE_BY_ONE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private final Context context;
    private final DirectorySubspace subspace;
    private final byte[] mutationTriggerKey;
    private final HybridLogicalClock hlc = new HybridLogicalClock();

    public ChangeLog(Context context, DirectorySubspace subspace) {
        this.context = context;
        this.subspace = subspace;
        this.mutationTriggerKey = subspace.pack(MUTATION_TRIGGER);
    }

    private void triggerWatchers(Transaction tr) {
        tr.mutate(MutationType.ADD, mutationTriggerKey, INCREASE_BY_ONE_DELTA);
    }

    private long getDateBucket() {
        // 1 day = 86_400_000 ms
        return context.now() / 86_400_000L;
    }

    private byte[] packValue(EntryMetadata metadata, Prefix prefix) {
        Tuple valueTuple = Tuple.from(metadata.segmentId(), metadata.position(), metadata.length(), prefix.asLong());
        return subspace.pack(valueTuple);
    }

    private void emitChangeWithMutation(Transaction tr, EntryMetadata metadata, Prefix prefix, Tuple keyTuple) {
        byte[] key = subspace.packWithVersionstamp(keyTuple);
        byte[] value = packValue(metadata, prefix);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value);
        triggerWatchers(tr);
    }

    private void emitChange(Transaction tr, EntryMetadata metadata, Prefix prefix, Tuple keyTuple) {
        byte[] key = subspace.pack(keyTuple);
        byte[] value = packValue(metadata, prefix);
        tr.set(key, value);
        triggerWatchers(tr);
    }

    private Tuple getKeyTuple(OperationKind kind, Versionstamp entryId) {
        long dateBucket = getDateBucket();
        long logSequenceNumber = hlc.next(context.now());
        return Tuple.from(
                CHANGELOG_SUBSPACE,
                dateBucket,
                logSequenceNumber,
                kind.getValue(),
                entryId
        );
    }

    public void appendOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, int userVersion) {
        Tuple tuple = getKeyTuple(OperationKind.APPEND, Versionstamp.incomplete(userVersion));
        emitChangeWithMutation(tr, metadata, prefix, tuple);
    }

    public void appendOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, Versionstamp versionstamp) {
        // Use this updating an existing entry.
        Tuple tuple = getKeyTuple(OperationKind.APPEND, versionstamp);
        emitChange(tr, metadata, prefix, tuple);
    }

    public void deleteOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, Versionstamp versionstamp) {
        Tuple tuple = getKeyTuple(OperationKind.DELETE, versionstamp);
        emitChange(tr, metadata, prefix, tuple);
    }
}
