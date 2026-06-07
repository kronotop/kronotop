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
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.*;

/**
 * Determines scan boundaries for compound index building and creates
 * {@link IndexBuildingTask}s for all shards.
 *
 * @see CompoundIndexUtil
 * @see BoundaryLocator
 */
public class CompoundIndexBoundaryRoutine extends AbstractBoundaryRoutine {

    public CompoundIndexBoundaryRoutine(Context context,
                                        DirectorySubspace subspace,
                                        Versionstamp taskId,
                                        IndexBoundaryTask task) {
        super(context, subspace, taskId, task);
    }

    @Override
    protected IndexHolder<?> lookupIndex(BucketMetadata metadata) {
        return metadata.compoundIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
    }

    @Override
    protected void saveDefinition(Transaction tr, BucketMetadata metadata, IndexDefinition definition) {
        CompoundIndexUtil.saveIndexDefinition(tr, metadata, (CompoundIndexDefinition) definition);
    }

    @Override
    protected void createBuildingTasks(TransactionalContext tx, BucketMetadata metadata, Boundaries boundaries) {
        for (int shardId : metadata.shards()) {
            CompoundIndexUtil.createCompoundIndexBuildingTask(tx, metadata, task.getIndexId(), shardId, boundaries);
        }
    }
}
