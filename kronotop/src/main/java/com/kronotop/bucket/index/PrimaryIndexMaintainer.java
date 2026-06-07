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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketMetadata;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * Maintains the primary (_id) index that maps each document's ObjectId to its {@link IndexEntry},
 * and manages versionstamped volume pointers used by the background index builder to track write progress.
 */
public final class PrimaryIndexMaintainer extends IndexMaintainer {

    /**
     * Inserts a primary index entry for a new document. Writes the ObjectId-to-IndexEntry mapping
     * in ENTRIES, adds a versionstamped volume pointer for background index builder tracking,
     * and increments the index cardinality.
     *
     * @param tr          the transaction to mutate
     * @param index       the resolved primary index
     * @param metadata    bucket metadata for cardinality updates
     * @param objectId    the document's ObjectId as bytes
     * @param indexEntry  the pre-encoded IndexEntry bytes
     * @param userVersion the user version for versionstamp ordering within the transaction
     */
    public static void setEntry(
            Transaction tr,
            Index index,
            BucketMetadata metadata,
            byte[] objectId,
            byte[] indexEntry,
            int userVersion
    ) {
        DirectorySubspace indexSubspace = index.subspace();

        // Primary Index Key Structure: (ENTRIES, ObjectId)
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId);
        byte[] key = indexSubspace.pack(tuple);
        tr.set(key, indexEntry);

        // Volume Pointer: (VOLUME_POINTER, Versionstamp, ObjectId)
        Tuple volumePointerTuple = Tuple.from(
                IndexSubspaceMagic.VOLUME_POINTER.getValue(),
                Versionstamp.incomplete(userVersion),
                objectId
        );
        byte[] volumePointer = indexSubspace.packWithVersionstamp(volumePointerTuple);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, volumePointer, NULL_VALUE);

        // Volume Pointer Back Reference: (VOLUME_POINTER_BACK_REF, ObjectId, Versionstamp)
        Tuple volumePointerBackRefTuple = Tuple.from(
                IndexSubspaceMagic.VOLUME_POINTER_BACK_REF.getValue(),
                objectId,
                Versionstamp.incomplete(userVersion)
        );
        byte[] volumePointerBackRef = indexSubspace.packWithVersionstamp(volumePointerBackRefTuple);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, volumePointerBackRef, NULL_VALUE);

        IndexUtil.mutateCardinality(tr, metadata.subspace(), index.definition().id(), 1);
    }

    /**
     * Resolves the Versionstamp associated with a document's ObjectId, returning a cached
     * result when available or reading from the VOLUME_POINTER_BACK_REF subspace on a miss.
     *
     * @param tr       the transaction to read from
     * @param index    the resolved primary index
     * @param objectId the document's ObjectId as bytes
     * @param metadata bucket metadata carrying the volume pointer cache
     * @return the Versionstamp for the given ObjectId, or null if not found
     */
    public static Versionstamp getVolumePointer(Transaction tr, Index index, byte[] objectId, BucketMetadata metadata) {
        ObjectId key = new ObjectId(objectId);
        Versionstamp cached = metadata.volumePointerCache().getIfPresent(key);
        if (cached != null) {
            return cached;
        }

        DirectorySubspace indexSubspace = index.subspace();
        byte[] prefix = indexSubspace.pack(
                Tuple.from(
                        IndexSubspaceMagic.VOLUME_POINTER_BACK_REF.getValue(),
                        objectId
                )
        );
        List<KeyValue> results = tr.getRange(Range.startsWith(prefix), 1).asList().join();
        if (results.isEmpty()) {
            return null;
        }
        Tuple unpacked = indexSubspace.unpack(results.getFirst().getKey());
        Versionstamp versionstamp = unpacked.getVersionstamp(2);
        metadata.volumePointerCache().put(key, versionstamp);
        return versionstamp;
    }

    /**
     * Removes a document's primary index entry from ENTRIES, clears its volume pointer,
     * and decrements the index cardinality.
     *
     * @param tr           the transaction to mutate
     * @param objectId     the ObjectId bytes of the document being deleted
     * @param versionstamp the document's versionstamp, used to locate its volume pointer key
     * @param index        the resolved primary index
     * @param metadata     bucket metadata for cardinality updates
     */
    public static void dropEntry(
            Transaction tr,
            byte[] objectId,
            Versionstamp versionstamp,
            Index index,
            BucketMetadata metadata
    ) {
        DirectorySubspace indexSubspace = index.subspace();

        // Volume Pointer: (VOLUME_POINTER, Versionstamp, ObjectId)
        byte[] volumePointerKey = indexSubspace.pack(
                Tuple.from(
                        IndexSubspaceMagic.VOLUME_POINTER.getValue(),
                        versionstamp,
                        objectId
                )
        );
        tr.clear(volumePointerKey);

        // Volume Pointer Back Reference: (VOLUME_POINTER_BACK_REF, ObjectId, Versionstamp)
        byte[] volumePointerBackRefKey = indexSubspace.pack(
                Tuple.from(
                        IndexSubspaceMagic.VOLUME_POINTER_BACK_REF.getValue(),
                        objectId,
                        versionstamp
                )
        );
        tr.clear(volumePointerBackRefKey);

        // Primary index itself.
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId);
        byte[] key = indexSubspace.pack(tuple);
        tr.clear(key);

        IndexUtil.mutateCardinality(tr, metadata.subspace(), index.definition().id(), -1);

        metadata.volumePointerCache().invalidate(new ObjectId(objectId));
    }

    /**
     * Overwrites the ENTRIES value for an existing document with a new {@link IndexEntry}.
     * Only the entry mapping is updated; the volume pointer and cardinality remain unchanged.
     *
     * @param tr            the transaction to mutate
     * @param objectId      the document's ObjectId as bytes
     * @param index         the resolved primary index
     * @param shardId       the shard containing the document
     * @param entryMetadata the new encoded metadata
     */
    public static void updateIndexEntry(
            Transaction tr,
            byte[] objectId,
            Index index,
            int shardId,
            byte[] entryMetadata
    ) {
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId);
        IndexEntry indexEntry = new IndexEntry(shardId, entryMetadata);

        DirectorySubspace indexSubspace = index.subspace();
        byte[] key = indexSubspace.pack(tuple);
        tr.set(key, indexEntry.encode());
    }
}
