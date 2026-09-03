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
import com.kronotop.volume.VolumeEntry;
import org.bson.BsonArray;
import org.bson.BsonNull;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds secondary (single-field) indexes on existing bucket data in the background.
 *
 * <p>Extends {@link AbstractBuildingRoutine} with index lookup via
 * {@link BucketMetadata#singleFieldIndexes()} and per-document entry insertion using
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

    @Override
    protected IndexHolder<?> lookupIndex(BucketMetadata metadata) {
        return metadata.singleFieldIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
    }

    @Override
    protected int indexBucketEntries(Transaction tr, BucketShard shard, BucketMetadata metadata, IndexBuildingTaskState state) {
        SingleFieldIndex index = metadata.singleFieldIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
        ScannedBatch batch = scanBatch(tr, shard, metadata, state, index);

        // Index only the entries that are not already present.
        for (BufferedEntry buffered : batch.buffer()) {
            if (batch.alreadyIndexed().contains(buffered.objectId())) {
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

        setCursor(tr, batch.lastVersionstamp());
        return batch.total();
    }
}
