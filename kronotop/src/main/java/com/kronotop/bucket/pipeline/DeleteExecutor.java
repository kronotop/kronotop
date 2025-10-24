/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CommitHook;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexBuilder;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.volume.DeleteResult;
import com.kronotop.volume.VolumeSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Executor responsible for performing document deletion operations in the Kronotop Cluster.
 *
 * <p>The DeleteExecutor processes a query pipeline to identify documents to be deleted,
 * then executes the actual deletion operations across multiple shards. It handles both
 * persisted entries and document locations by grouping them by shard ID and performing
 * volume deletion operations on each affected shard.
 *
 * <p>The execution process involves:
 * <ol>
 *   <li>Executing the underlying pipeline to populate data sinks</li>
 *   <li>Extracting versionstamps from the populated sinks</li>
 *   <li>Grouping versionstamps by their associated shard ID</li>
 *   <li>Performing volume deletion operations for each shard</li>
 *   <li>Returning the list of all deleted document versionstamps</li>
 * </ol>
 *
 * <p>This executor supports deletion from two types of data sinks:
 * <ul>
 *   <li>{@link PersistedEntrySink} - Contains complete document entries with metadata</li>
 *   <li>{@link DocumentLocationSink} - Contains document location information</li>
 * </ul>
 *
 * @see Executor
 * @see PipelineExecutor
 * @see DataSink
 */
public final class DeleteExecutor extends BaseExecutor implements Executor<List<Versionstamp>> {
    private final PipelineExecutor executor;

    /**
     * Constructs a new DeleteExecutor with the specified pipeline executor.
     *
     * @param executor the pipeline executor used to process the query and populate data sinks
     */
    public DeleteExecutor(PipelineExecutor executor) {
        this.executor = executor;
    }

    /**
     * Executes the deletion operation by processing the pipeline and deleting identified documents.
     *
     * <p>This method first executes the underlying pipeline to populate data sinks with documents
     * to be deleted. It then processes these sinks to extract versionstamps, groups them by
     * shard ID, and performs the actual volume deletion operations on each affected shard.
     *
     * <p>The method ensures proper cleanup by clearing the data sink after processing,
     * even if an exception occurs during deletion.
     *
     * @param tr  the FoundationDB transaction to use for deletion operations
     * @param ctx the query context containing the pipeline plan and execution environment
     * @return a list of versionstamps for all successfully deleted documents
     * @throws RuntimeException if deletion operations fail on any shard
     */
    @Override
    public List<Versionstamp> execute(Transaction tr, QueryContext ctx) {
        executor.execute(tr, ctx);

        PipelineNode head = findHeadNode(ctx.plan());

        if (head == null) {
            return List.of();
        }

        DataSink sink = ctx.sinks().load(head.id());
        if (sink == null) {
            return List.of();
        }

        try {
            Map<Integer, List<Versionstamp>> byShardId = accumulateAndGroupVersionstampsByShardId(sink);

            // Collected all affected versionstamps and grouped by ShardID.
            List<Versionstamp> versionstamps = new ArrayList<>();
            for (Map.Entry<Integer, List<Versionstamp>> item : byShardId.entrySet()) {
                BucketShard shard = ctx.env().bucketService().getShard(item.getKey());
                VolumeSession session = new VolumeSession(tr, ctx.metadata().volumePrefix());
                versionstamps.addAll(item.getValue());
                DeleteResult deleteResult = shard.volume().delete(session, item.getValue().toArray(new Versionstamp[0]));
                PostCommitHook postCommitHook = new PostCommitHook(deleteResult);
                ctx.registerPostCommitHook(postCommitHook);
            }

            for (Index index : ctx.metadata().indexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
                // Skip the default ID index as it'll be handled separately
                if (index.definition().equals(DefaultIndexDefinition.ID)) {
                    continue;
                }
                for (Versionstamp versionstamp : versionstamps) {
                    IndexBuilder.dropIndexEntry(tr, versionstamp, index.definition(), index.subspace(), ctx.metadata().subspace());
                }
            }

            for (Versionstamp versionstamp : versionstamps) {
                IndexBuilder.dropPrimaryIndexEntry(tr, versionstamp, ctx.metadata());
            }

            return versionstamps;
        } finally {
            sink.clear();
        }
    }

    private record PostCommitHook(DeleteResult deleteResult) implements CommitHook {
        @Override
        public void run() {
            deleteResult.complete();
        }
    }
}
