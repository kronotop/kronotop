package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Executor responsible for performing document read operations in the Kronotop Cluster.
 * 
 * <p>The ReadExecutor processes a query pipeline to identify documents to be retrieved,
 * then executes the actual read operations to fetch document content. It handles both
 * persisted entries (where documents are already in memory) and document locations
 * (where documents need to be retrieved from storage).
 * 
 * <p>The execution process involves:
 * <ol>
 *   <li>Executing the underlying pipeline to populate data sinks</li>
 *   <li>Processing the populated sinks to extract document data</li>
 *   <li>For persisted entries: directly accessing document content</li>
 *   <li>For document locations: retrieving documents from storage via DocumentRetriever</li>
 *   <li>Returning a map of versionstamps to document content</li>
 * </ol>
 * 
 * <p>This executor supports reading from two types of data sinks:
 * <ul>
 *   <li>{@link PersistedEntrySink} - Contains complete document entries already in memory</li>
 *   <li>{@link DocumentLocationSink} - Contains document location metadata requiring retrieval</li>
 * </ul>
 * 
 * <p>The returned map maintains insertion order using {@link LinkedHashMap}, preserving
 * the order in which documents were processed from the pipeline.
 * 
 * @see Executor
 * @see PipelineExecutor
 * @see DataSink
 */
public final class ReadExecutor implements Executor<Map<Versionstamp, ByteBuffer>> {
    private final PipelineExecutor executor;

    /**
     * Constructs a new ReadExecutor with the specified pipeline executor.
     * 
     * @param executor the pipeline executor used to process the query and populate data sinks
     */
    public ReadExecutor(PipelineExecutor executor) {
        this.executor = executor;
    }

    /**
     * Executes the read operation by processing the pipeline and retrieving identified documents.
     * 
     * <p>This method first executes the underlying pipeline to populate data sinks with documents
     * to be read. It then processes these sinks to extract document content, either directly
     * from memory (for persisted entries) or by retrieving from storage (for document locations).
     * 
     * <p>The method ensures proper cleanup by clearing the data sink after processing,
     * even if an exception occurs during document retrieval.
     * 
     * @param tr the FoundationDB transaction to use for read operations
     * @param ctx the query context containing the pipeline plan and execution environment
     * @return a map of versionstamps to document content (ByteBuffer) for all retrieved documents,
     *         maintaining insertion order
     * @throws RuntimeException if document retrieval operations fail
     */
    @Override
    public Map<Versionstamp, ByteBuffer> execute(Transaction tr, QueryContext ctx) {
        executor.execute(tr, ctx);

        PipelineNode plan = ctx.plan();
        if (plan == null) {
            return Map.of();
        }

        DataSink sink = ctx.sinks().load(plan.id());
        if (sink == null) {
            return Map.of();
        }

        Map<Versionstamp, ByteBuffer> result = new LinkedHashMap<>();
        try {
            return switch (sink) {
                case PersistedEntrySink persistedEntrySink -> {
                    persistedEntrySink.forEach((versionstamp, entry) -> {
                        result.put(versionstamp, entry.document());
                    });
                    yield result;
                }
                case DocumentLocationSink documentLocationSink -> {
                    documentLocationSink.forEach((entryMetadataId, location) -> {
                        ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.metadata(), location);
                        result.put(location.versionstamp(), document);
                    });
                    yield result;
                }
            };
        } finally {
            sink.clear();
        }
    }
}
