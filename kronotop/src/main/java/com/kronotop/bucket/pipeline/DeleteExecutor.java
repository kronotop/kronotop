package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.index.IndexBuilder;
import com.kronotop.bucket.index.IndexDefinition;
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
                shard.volume().delete(session, item.getValue().toArray(new Versionstamp[0]));
            }

            // TODO: This code will be removed when we refactor how we store indexes in BucketMetadata
            for (String selector : ctx.metadata().indexes().getSelectors()) {
                for (Versionstamp versionstamp : versionstamps) {
                    IndexDefinition definition = ctx.metadata().indexes().getIndexBySelector(selector);
                    DirectorySubspace indexSubspace = ctx.metadata().indexes().getSubspace(selector);
                    IndexBuilder.dropIndexEntry(tr, versionstamp, definition, indexSubspace, ctx.metadata().subspace());
                }
            }

            for (Versionstamp versionstamp : versionstamps) {
                IndexBuilder.dropPrimaryIndex(tr, versionstamp, ctx.metadata());
            }

            return versionstamps;
        } finally {
            sink.clear();
        }
    }
}
