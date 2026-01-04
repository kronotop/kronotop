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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
public final class ReadExecutor extends BaseExecutor implements Executor<Map<Versionstamp, ByteBuffer>> {
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
     * @param tr  the FoundationDB transaction to use for read operations
     * @param ctx the query context containing the pipeline plan and execution environment
     * @return a map of versionstamps to document content (ByteBuffer) for all retrieved documents,
     * maintaining insertion order
     * @throws RuntimeException if document retrieval operations fail
     */
    @Override
    public Map<Versionstamp, ByteBuffer> execute(Transaction tr, QueryContext ctx) {
        executor.execute(tr, ctx);

        PipelineNode head = findHeadNode(ctx.plan());

        if (head == null) {
            return Map.of();
        }

        DataSink sink = ctx.sinks().load(head.id());
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
                    // Phase 1: Collect all locations
                    List<Versionstamp> versionstamps = new ArrayList<>();
                    List<DocumentLocation> locations = new ArrayList<>();
                    documentLocationSink.forEach((entryHandle, location) -> {
                        versionstamps.add(location.versionstamp());
                        locations.add(location);
                    });

                    if (locations.isEmpty()) {
                        yield result;
                    }

                    // Phase 2: Batch retrieve all documents
                    List<ByteBuffer> documents = ctx.env().documentRetriever()
                            .retrieveDocuments(ctx.metadata(), locations);

                    // Phase 3: Build result map
                    for (int i = 0; i < documents.size(); i++) {
                        result.put(versionstamps.get(i), documents.get(i));
                    }
                    yield result;
                }
            };
        } finally {
            sink.clear();
        }
    }
}
