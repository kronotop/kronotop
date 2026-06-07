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
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.ReservedFieldName;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.statistics.IndexStatsBuilder;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * Builds compound indexes on existing bucket data in the background.
 *
 * <p>Extends {@link AbstractBuildingRoutine} with index lookup via
 * {@link BucketMetadata#compoundIndexes()} and per-document entry insertion using
 * {@link CompoundIndexMaintainer}.
 *
 * @see AbstractBuildingRoutine
 * @see CompoundIndexMaintainer
 */
public class CompoundIndexBuildingRoutine extends AbstractBuildingRoutine {

    public CompoundIndexBuildingRoutine(
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
        return metadata.compoundIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
    }

    @Override
    protected int indexBucketEntries(Transaction tr, BucketShard shard, BucketMetadata metadata, IndexBuildingTaskState state) {
        int total = 0;

        VersionstampedKeySelector begin = !state.bootstrapped() ?
                VersionstampedKeySelector.firstGreaterOrEqual(state.cursorVersionstamp()) :
                VersionstampedKeySelector.firstGreaterThan(state.cursorVersionstamp());

        VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(task.getUpper());
        VolumeSession session = new VolumeSession(tr, metadata.prefix());

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
        Iterable<VolumeEntry> entries = shard.volume().getRange(session, begin, end, INDEX_SCAN_BATCH_SIZE);
        Versionstamp versionstamp = null;
        for (VolumeEntry pair : entries) {
            checkForShutdown();

            total++;
            versionstamp = pair.key();

            BsonValue idValue = SelectorMatcher.match(ReservedFieldName.ID.getValue(), pair.entry());
            if (idValue == null || !idValue.isObjectId()) {
                throw new IndexMaintenanceRoutineException("Document missing _id field or _id is not an ObjectId");
            }
            ObjectId objectId = idValue.asObjectId().getValue();
            byte[] objectIdBytes = objectId.toByteArray();

            List<List<Object>> valueCombinations = CompoundIndexMaintainer.extractFieldValues(compoundIndex, pair.entry(), strictTypes);
            for (List<Object> fieldValues : valueCombinations) {
                CompoundIndexMaintainer.insertEntry(tr, compoundIndex, metadata, versionstamp,
                        objectIdBytes, fieldValues, shardId, pair.metadata(), service.getCollatorCache());
            }
            IndexStatsBuilder.setHintForStatsIfSelected(tr, compoundIndex, objectIdBytes);
        }

        if (!state.bootstrapped()) {
            IndexBuildingTaskState.setBootstrapped(tr, subspace, taskId, true);
        }

        setCursor(tr, versionstamp);
        return total;
    }
}
