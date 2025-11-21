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

/**
 * Tracks volume operations in FoundationDB with temporal ordering using Hybrid Logical Clock.
 * Provides append and delete operation tracking with automatic watcher notification for replication.
 */
public class ChangeLog {
    private final Context context;
    private final DirectorySubspace subspace;
    private final HybridLogicalClock hlc = new HybridLogicalClock();

    /**
     * Creates a new ChangeLog instance.
     *
     * @param context  the context providing FoundationDB access and system time
     * @param subspace the directory subspace for storing changelog entries
     */
    public ChangeLog(Context context, DirectorySubspace subspace) {
        this.context = context;
        this.subspace = subspace;
    }

    /**
     * Calculates the current date bucket for partitioning changelog entries by day.
     *
     * @return date bucket value (days since epoch)
     */
    private long getDateBucket() {
        // 1 day = 86_400_000 ms
        return context.now() / 86_400_000L;
    }

    /**
     * Packs entry metadata into a tuple value.
     *
     * @param metadata the entry metadata to pack
     * @param prefix   the prefix associated with the entry
     * @return packed tuple bytes
     */
    private byte[] packValue(EntryMetadata metadata, Prefix prefix) {
        Tuple valueTuple = Tuple.from(metadata.segmentId(), metadata.position(), metadata.length(), prefix.asLong());
        return valueTuple.pack();
    }

    /**
     * Emits a changelog entry using a versionstamped key mutation.
     *
     * @param tr       the transaction to use
     * @param metadata the entry metadata
     * @param prefix   the prefix associated with the entry
     * @param keyTuple the key tuple containing an incomplete versionstamp
     */
    private void emitChangeWithMutation(Transaction tr, EntryMetadata metadata, Prefix prefix, Tuple keyTuple) {
        byte[] key = subspace.packWithVersionstamp(keyTuple);
        byte[] value = packValue(metadata, prefix);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value);
        triggerWatchers(tr);
    }

    /**
     * Emits a changelog entry using a regular set operation.
     *
     * @param tr       the transaction to use
     * @param metadata the entry metadata
     * @param prefix   the prefix associated with the entry
     * @param keyTuple the key tuple with a complete versionstamp
     */
    private void emitChange(Transaction tr, EntryMetadata metadata, Prefix prefix, Tuple keyTuple) {
        byte[] key = subspace.pack(keyTuple);
        byte[] value = packValue(metadata, prefix);
        tr.set(key, value);
        triggerWatchers(tr);
    }

    /**
     * Constructs a key tuple for changelog entry.
     *
     * @param kind    the operation kind
     * @param entryId the versionstamp identifying the entry
     * @return key tuple containing all components
     */
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

    /**
     * Records an append operation for a new entry with an incomplete versionstamp.
     *
     * @param tr          the transaction to use
     * @param metadata    the entry metadata
     * @param prefix      the prefix associated with the entry
     * @param userVersion the user version for the versionstamp
     */
    public void appendOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, int userVersion) {
        Tuple tuple = getKeyTuple(OperationKind.APPEND, Versionstamp.incomplete(userVersion));
        emitChangeWithMutation(tr, metadata, prefix, tuple);
    }

    /**
     * Records an append operation for an existing entry with complete versionstamp.
     *
     * @param tr           the transaction to use
     * @param metadata     the entry metadata
     * @param prefix       the prefix associated with the entry
     * @param versionstamp the complete versionstamp
     */
    public void appendOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, Versionstamp versionstamp) {
        Tuple tuple = getKeyTuple(OperationKind.APPEND, versionstamp);
        emitChange(tr, metadata, prefix, tuple);
    }

    /**
     * Records a delete operation for an entry.
     *
     * @param tr           the transaction to use
     * @param metadata     the entry metadata
     * @param prefix       the prefix associated with the entry
     * @param versionstamp the complete versionstamp
     */
    public void deleteOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, Versionstamp versionstamp) {
        Tuple tuple = getKeyTuple(OperationKind.DELETE, versionstamp);
        emitChange(tr, metadata, prefix, tuple);
    }
}
