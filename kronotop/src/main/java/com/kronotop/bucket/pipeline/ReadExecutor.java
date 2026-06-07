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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.ibm.icu.text.Collator;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.TypeBracketComparator;
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.SelectorMatcher;
import com.kronotop.internal.StringUtil;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Executor responsible for performing document read operations in the Kronotop Cluster.
 *
 * <p>The ReadExecutor processes a query pipeline to identify documents to be retrieved,
 * then executes the actual read operations to fetch document content. It handles both
 * persisted entries (where documents are already in memory) and document locations
 * (where documents need to be retrieved from storage).
 *
 * <p>This executor supports reading from two types of data sinks:
 * <ul>
 *   <li>{@link PersistedEntrySink} - Contains complete document entries already in memory</li>
 *   <li>{@link DocumentLocationSink} - Contains document location metadata requiring retrieval</li>
 * </ul>
 *
 * @see Executor
 * @see PipelineExecutor
 * @see DataSink
 */
public final class ReadExecutor extends BaseExecutor implements Executor<List<ByteBuffer>> {
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
     * @param tr  the FoundationDB transaction to use for read operations
     * @param ctx the query context containing the pipeline plan and execution environment
     * @return a list of document content (ByteBuffer) for all retrieved documents
     * @throws RuntimeException if document retrieval operations fail
     */
    @Override
    public List<ByteBuffer> execute(Transaction tr, QueryContext ctx) {
        executor.execute(tr, ctx);

        PipelineNode head = findHeadNode(ctx.plan());

        if (head == null) {
            return List.of();
        }

        DataSink sink = ctx.sinks().load(head.id());
        if (sink == null) {
            return List.of();
        }

        List<ByteBuffer> result = new ArrayList<>();
        try {
            return switch (sink) {
                case PersistedEntrySink persistedEntrySink -> {
                    for (PersistedEntry entry : persistedEntrySink.entries()) {
                        result.add(entry.document());
                    }
                    yield applyLimit(ctx, resultSortIfNeeded(ctx, result));
                }
                case DocumentLocationSink documentLocationSink -> {
                    // Phase 1: Deduplicate by ObjectId only when a multi-key index produced the results
                    if (ctx.isScannedIndexMultiKey()) {
                        documentLocationSink.dedupByObjectId();
                    }

                    if (documentLocationSink.size() == 0) {
                        yield result;
                    }

                    // Phase 2: Batch retrieve all documents
                    List<ByteBuffer> documents = ctx.env().documentRetriever().retrieveDocuments(documentLocationSink.entries());

                    result.addAll(documents);

                    // Phase 3: Apply in-memory sorting if needed
                    yield applyLimit(ctx, resultSortIfNeeded(ctx, result));
                }
            };
        } finally {
            sink.clear();
        }
    }

    private List<ByteBuffer> applyLimit(QueryContext ctx, List<ByteBuffer> result) {
        int limit = ctx.options().limit();
        if (result.size() > limit) {
            return result.subList(0, limit);
        }
        return result;
    }

    /**
     * Applies in-memory sorting when RESULTSORT is specified.
     * Uses decorate-sort-undecorate to extract sort keys once instead of per comparison.
     */
    private List<ByteBuffer> resultSortIfNeeded(QueryContext ctx, List<ByteBuffer> result) {
        if (result.size() <= 1) {
            return result;
        }

        String resultSortByField = ctx.options().getResultSortField();
        if (resultSortByField == null) {
            return result;
        }

        SortDirection direction = ctx.options().getResultSortDirection();

        Collation collation = CollationResolver.resolve(ctx.metadata(), resultSortByField, ctx.options().getCollation());
        Collator collator = collation != null ? ctx.env().collatorCache().acquire(collation) : null;

        // Decorate: extract sort keys once (O(N) extractions)
        String[] sortSegments = StringUtil.split(resultSortByField);
        List<SortEntry> decorated = new ArrayList<>(result.size());
        for (ByteBuffer doc : result) {
            BsonValue key = SelectorMatcher.match(sortSegments, doc);
            decorated.add(new SortEntry(key, doc));
        }

        // Sort: compare pre-extracted keys only
        decorated.sort((e1, e2) -> {
            BsonValue a = e1.key();
            BsonValue b = e2.key();
            int cmp;
            if (collator != null && a != null && b != null
                    && a.getBsonType() == BsonType.STRING && b.getBsonType() == BsonType.STRING) {
                cmp = collator.compare(a.asString().getValue(), b.asString().getValue());
            } else {
                cmp = TypeBracketComparator.INSTANCE.compare(a, b);
            }
            return direction == SortDirection.DESC ? -cmp : cmp;
        });

        // Undecorate: collect documents in sorted order
        List<ByteBuffer> sorted = new ArrayList<>(decorated.size());
        for (SortEntry entry : decorated) {
            sorted.add(entry.document());
        }
        return sorted;
    }

    private record SortEntry(BsonValue key, ByteBuffer document) {
    }
}
