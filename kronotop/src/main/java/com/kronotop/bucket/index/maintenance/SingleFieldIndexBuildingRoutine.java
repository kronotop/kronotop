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

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.statistics.IndexStatsBuilder;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import org.bson.BsonArray;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds secondary (single-field) indexes on existing bucket data in the background.
 *
 * <p>Extends {@link AbstractBuildingRoutine} with index lookup via
 * {@link BucketMetadata#indexes()} and per-document entry insertion using
 * {@link SingleFieldIndexMaintainer}, including multikey array handling.
 *
 * @see AbstractBuildingRoutine
 * @see SingleFieldIndexMaintainer
 */
public class SingleFieldIndexBuildingRoutine extends AbstractBuildingRoutine {

    public SingleFieldIndexBuildingRoutine(
            Context context,
            DirectorySubspace subspace,
            int shardId,
            Versionstamp taskId,
            IndexBuildingTask task
    ) {
        super(context, subspace, shardId, taskId, task);
    }

    private record BufferedEntry(VolumeEntry entry, ObjectId objectId, byte[] objectIdBytes) {
    }

    @Override
    protected IndexHolder<?> lookupIndex(BucketMetadata metadata) {
        return metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
    }

    @Override
    protected int indexBucketEntries(Transaction tr, BucketShard shard, BucketMetadata metadata, IndexBuildingTaskState state) {
        int total = 0;

        VersionstampedKeySelector begin = !state.bootstrapped() ?
                VersionstampedKeySelector.firstGreaterOrEqual(state.cursorVersionstamp()) :
                VersionstampedKeySelector.firstGreaterThan(state.cursorVersionstamp());

        VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(task.getUpper());
        VolumeSession session = new VolumeSession(tr, metadata.prefix());

        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
        Iterable<VolumeEntry> entries = shard.volume().getRange(session, begin, end, INDEX_SCAN_BATCH_SIZE);

        // Drain the batch and extract ObjectIds.
        List<BufferedEntry> buffer = new ArrayList<>();
        Versionstamp versionstamp = null;
        for (VolumeEntry pair : entries) {
            checkForShutdown();

            total++;
            versionstamp = pair.key();

            // Extract ObjectId from document's _id field
            BsonValue idValue = SelectorMatcher.match(ReservedFieldName.ID.getValue(), pair.entry());
            if (idValue == null || !idValue.isObjectId()) {
                throw new IndexMaintenanceRoutineException("Document missing _id field or _id is not an ObjectId");
            }
            ObjectId objectId = idValue.asObjectId().getValue();
            buffer.add(new BufferedEntry(pair, objectId, objectId.toByteArray()));
        }

        // Find which ObjectIds in this batch are already indexed (e.g., by online writers),
        // so they are not processed again and cardinality is not double-counted.
        Set<ObjectId> alreadyIndexed = IndexMaintainer.findIndexedObjectIds(
                tr, index.subspace(), buffer.stream().map(BufferedEntry::objectIdBytes).toList());

        // Index only the entries that are not already present.
        for (BufferedEntry buffered : buffer) {
            if (alreadyIndexed.contains(buffered.objectId())) {
                continue;
            }
            VolumeEntry pair = buffered.entry();
            byte[] objectIdBytes = buffered.objectIdBytes();

            BsonValue bsonValue = SelectorMatcher.match(index.definition().selector(), pair.entry());

            if (bsonValue instanceof BsonArray bsonArray) {
                // Multikey index: create an index entry for each unique value in the array
                Set<Object> uniqueIndexValues = new HashSet<>();
                List<BsonValue> uniqueBsonValues = new ArrayList<>();
                boolean hasNull = false;
                for (BsonValue element : bsonArray) {
                    if (element == null || element.equals(BsonNull.VALUE)) {
                        hasNull = true;
                        continue;
                    }
                    Object indexValue = BSONUtil.toObject(element, index.definition().bsonType());
                    if (indexValue == null) {
                        if (strictTypes) {
                            throw new IndexTypeMismatchException(index.definition(), element);
                        }
                        continue;
                    }
                    if (uniqueIndexValues.add(indexValue)) {
                        uniqueBsonValues.add(element);
                    }
                }
                // Index null elements (deduplicated) for consistent semantics with single-value indexes
                if (hasNull && uniqueIndexValues.add(null)) {
                    uniqueBsonValues.add(BsonNull.VALUE);
                }
                for (Object indexValue : uniqueIndexValues) {
                    SingleFieldIndexMaintainer.insertEntry(tr, index, metadata,
                            objectIdBytes, indexValue, shardId, pair.metadata(), service.getCollatorCache());
                }
                // Track stats for each unique element
                for (BsonValue element : uniqueBsonValues) {
                    IndexStatsBuilder.setHintForStats(tr, index, objectIdBytes, element);
                }
            } else {
                // Single value index
                Object indexValue = null;
                if (bsonValue != null && !bsonValue.equals(BsonNull.VALUE)) {
                    indexValue = BSONUtil.toObject(bsonValue, index.definition().bsonType());
                    if (indexValue == null) {
                        if (!strictTypes) {
                            // Type mismatch, continue
                            continue;
                        }
                        throw new IndexTypeMismatchException(index.definition(), bsonValue);
                    }
                }

                SingleFieldIndexMaintainer.insertEntry(tr, index, metadata,
                        objectIdBytes, indexValue, shardId, pair.metadata(), service.getCollatorCache());
                IndexStatsBuilder.setHintForStats(tr, index, objectIdBytes, bsonValue);
            }
        }

        if (!state.bootstrapped()) {
            IndexBuildingTaskState.setBootstrapped(tr, subspace, taskId, true);
        }

        setCursor(tr, versionstamp);
        return total;
    }
}
