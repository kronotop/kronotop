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

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.index.*;
import com.kronotop.volume.*;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BucketEntryEvacuator implements EntryEvacuator {
    private final Volume volume;
    private final int shardId;
    private final int maxEntries;
    private final long maxBytes;
    private int entryCount;
    private long totalBytes;

    public BucketEntryEvacuator(Volume volume, int shardId, int maxEntries, long maxBytes) {
        this.volume = volume;
        this.shardId = shardId;
        this.maxEntries = maxEntries;
        this.maxBytes = maxBytes;
    }

    public BucketEntryEvacuator(Volume volume, int shardId) {
        this(volume, shardId, 100, 32 * 1024 * 1024);
    }

    private void updateIndexes(Transaction tr, BucketMetadata metadata, byte[] objectId, byte[] encodedMetadata) {
        IndexEntry indexEntry = new IndexEntry(shardId, encodedMetadata);
        byte[] encodedIndexEntry = indexEntry.encode();

        // Primary Index
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);
        PrimaryIndexMaintainer.updateIndexEntry(tr, objectId, primaryIndex, shardId, encodedMetadata);

        // Single field indexes
        for (Index index : metadata.indexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
            if (PrimaryIndex.isPrimary(index.definition())) {
                continue;
            }
            SingleFieldIndexMaintainer.updateIndexEntry(tr, objectId, encodedIndexEntry, index.subspace());
        }

        // Compound indexes
        for (CompoundIndex compoundIndex : metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
            CompoundIndexMaintainer.updateIndexEntry(
                    tr,
                    objectId,
                    encodedIndexEntry,
                    compoundIndex.subspace(),
                    compoundIndex.definition().fields().size()
            );
        }

        // Vector indexes
        for (VectorIndex vectorIndex : metadata.vectorIndexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
            VectorIndexMaintainer.updateIndexEntry(tr, objectId, vectorIndex, shardId, encodedMetadata);
        }
    }

    @Override
    public boolean evacuate(
            Context context,
            Transaction tr,
            EntryMetadata entryMetadata,
            Versionstamp versionstamp
    ) throws IOException {
        // TODO: Not cached for now. Fix the cache invalidation issues first, then revisit this.
        BucketMetadata metadata = BucketMetadataUtil.openByPrefix(context, tr, entryMetadata.prefix());

        ByteBuffer document = volume.getByEntryMetadata(entryMetadata);

        BsonDocument doc = BSONUtil.fromBson(document.duplicate());
        ObjectId objectId = doc.get(ReservedFieldName.ID.getValue()).asObjectId().getValue();

        VolumeSession session = new VolumeSession(tr, metadata.prefix());
        VersionstampedEntryUpdate update = new VersionstampedEntryUpdate(versionstamp, entryMetadata, document);
        UpdateResult result = volume.updateByVersionstampedEntryUpdate(session, update);

        UpdatedEntry updatedEntry = result.entries()[0];
        updateIndexes(tr, metadata, objectId.toByteArray(), updatedEntry.encodedMetadata());

        entryCount++;
        totalBytes += entryMetadata.length();

        if (entryCount >= maxEntries || totalBytes >= maxBytes) {
            entryCount = 0;
            totalBytes = 0;
            return false;
        }
        return true;
    }
}
