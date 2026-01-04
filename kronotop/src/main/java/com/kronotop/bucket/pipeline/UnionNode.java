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

import com.apple.foundationdb.tuple.Versionstamp;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a logical OR operation in the query execution pipeline.
 * <p>
 * UnionNode combines results from multiple child scan nodes using set union semantics.
 * When a query contains an OR condition (e.g., {@code {$or: [{age: 25}, {city: "NYC"}]}}),
 * each branch executes independently and UnionNode merges their results, ensuring each
 * matching document appears exactly once in the final output.
 * <p>
 * The implementation uses RoaringBitmap for efficient set operations. Each child node
 * produces a set of entry handles (unique document identifiers), and these sets are
 * combined via bitwise OR. This approach is significantly faster than naive list merging
 * for large result sets.
 * <p>
 * UnionNode handles heterogeneous child outputs: some children may produce
 * {@link PersistedEntrySink} (full document data) while others produce
 * {@link DocumentLocationSink} (location pointers requiring document retrieval).
 * The node normalizes these into a unified output by fetching documents as needed.
 *
 * @see IntersectionNode for AND semantics
 * @see LogicalNode for the logical node contract
 */
public class UnionNode extends AbstractLogicalNode implements LogicalNode {
    public UnionNode(int id, List<PipelineNode> children) {
        super(id, children);
    }

    /**
     * Determines if all child nodes have completed execution and marks this node
     * as exhausted accordingly.
     * <p>
     * Union semantics require processing all children before producing final results.
     * This method enables incremental execution by tracking which children still
     * have data to contribute.
     *
     * @param ctx the query context managing execution state
     * @return {@code true} if all children are exhausted and this node is now marked exhausted,
     *         {@code false} if at least one child has more results to produce
     */
    private boolean checkAndPropagateExhaustion(QueryContext ctx) {
        for (PipelineNode child : children()) {
            ExecutionState childState = ctx.getOrCreateExecutionState(child.id());
            if (!childState.isExhausted()) {
                return false;
            }
        }
        ctx.getOrCreateExecutionState(id()).setExhausted(true);
        return true;
    }

    /**
     * Combines multiple bitmaps into a single bitmap using bitwise OR.
     * <p>
     * This aggregates entry handles from all child nodes into one set representing
     * all documents that match any of the OR conditions. Null bitmaps are safely
     * skipped, allowing partial results from children that produced no matches.
     *
     * @param bitmaps array of bitmaps from child nodes, may contain nulls
     * @return a new bitmap containing the union of all input bitmaps
     */
    private Roaring64Bitmap orAll(Roaring64Bitmap[] bitmaps) {
        Roaring64Bitmap result = new Roaring64Bitmap();
        if (bitmaps == null) {
            return result;
        }
        for (Roaring64Bitmap bm : bitmaps) {
            if (bm != null) {
                result.or(bm);
            }
        }
        return result;
    }

    /**
     * Executes the union operation by merging results from all child nodes.
     * <p>
     * The execution proceeds in three phases:
     * <ol>
     *   <li><b>Collection</b>: Iterates through each child's data sink, extracting entry handles
     *       and document metadata. Builds a bitmap per child for efficient set operations,
     *       while maintaining a map of document pointers for later retrieval.</li>
     *   <li><b>Union</b>: Combines all child bitmaps via OR operation to produce the final
     *       set of unique document handles.</li>
     *   <li><b>Materialization</b>: For each handle in the union, retrieves the document body
     *       (either from cached data or by fetching from storage) and writes to this node's
     *       output sink.</li>
     * </ol>
     * <p>
     * Child sinks are cleared after processing to release memory. The method exits early
     * if all children are exhausted, preventing redundant work in cursor-based pagination.
     *
     * @param ctx the query context providing access to sinks, document retriever, and execution state
     * @throws IllegalStateException if a child node has no associated data sink
     */
    @Override
    public void execute(QueryContext ctx) {
        if (checkAndPropagateExhaustion(ctx)) {
            // All children are exhausted
            return;
        }

        Map<Long, DocumentPointer> result = new LinkedHashMap<>();
        Roaring64Bitmap[] bitmaps = new Roaring64Bitmap[children().size()];

        for (int index = 0; index < children().size(); index++) {
            PipelineNode child = children().get(index);
            PipelineNode head = findHeadNode(child);
            DataSink sink = ctx.sinks().load(head.id());
            if (sink == null) {
                throw new IllegalStateException("No data sink found for " + child);
            }

            Roaring64Bitmap bitmap = new Roaring64Bitmap();
            switch (sink) {
                case PersistedEntrySink persistedEntrySink -> {
                    persistedEntrySink.forEach((versionstamp, persistedEntry) -> {
                        bitmap.add(persistedEntry.handle());
                        result.compute(persistedEntry.handle(), (ignored, documentPointer) -> {
                            if (documentPointer == null) {
                                DocumentPointer pointer = new DocumentPointer();
                                pointer.setVersionstamp(versionstamp);
                                pointer.setPersistedEntry(persistedEntry);
                                return pointer;
                            }
                            documentPointer.setVersionstamp(versionstamp);
                            documentPointer.setPersistedEntry(persistedEntry);
                            return documentPointer;
                        });
                    });
                }
                case DocumentLocationSink documentLocationSink ->
                        documentLocationSink.forEach((entryHandle, location) -> {
                            bitmap.add(entryHandle);
                            result.compute(entryHandle, (ignored, documentPointer) -> {
                                if (documentPointer == null) {
                                    DocumentPointer pointer = new DocumentPointer();
                                    pointer.setLocation(location);
                                    return pointer;
                                }
                                documentPointer.setLocation(location);
                                return documentPointer;
                            });
                        });
            }

            bitmaps[index] = bitmap;
        }
        Roaring64Bitmap union = orAll(bitmaps);

        // Phase 1: Identify documents needing retrieval
        List<Long> handlesNeedingRetrieval = new ArrayList<>();
        List<DocumentLocation> locationsNeedingRetrieval = new ArrayList<>();

        LongIterator it = union.getLongIterator();
        while (it.hasNext()) {
            long entryHandle = it.next();
            DocumentPointer pointer = result.get(entryHandle);
            if (pointer.getPersistedEntry() == null) {
                handlesNeedingRetrieval.add(entryHandle);
                locationsNeedingRetrieval.add(pointer.getLocation());
            }
        }

        // Phase 2: Batch retrieve missing documents
        Long2ObjectMap<ByteBuffer> retrievedByHandle = new Long2ObjectOpenHashMap<>();
        if (!locationsNeedingRetrieval.isEmpty()) {
            List<ByteBuffer> retrievedDocuments = ctx.env().documentRetriever().retrieveDocuments(
                    ctx.metadata(), locationsNeedingRetrieval
            );
            for (int i = 0; i < handlesNeedingRetrieval.size(); i++) {
                retrievedByHandle.put((long) handlesNeedingRetrieval.get(i), retrievedDocuments.get(i));
            }
        }

        // Phase 3: Write all results
        DataSink sink = ctx.sinks().loadOrCreatePersistedEntrySink(id());
        it = union.getLongIterator();
        while (it.hasNext()) {
            long entryHandle = it.next();
            DocumentPointer pointer = result.get(entryHandle);
            if (pointer.getPersistedEntry() != null) {
                ctx.sinks().writePersistedEntry(sink, pointer.getVersionstamp(), pointer.getPersistedEntry());
            } else {
                ByteBuffer document = retrievedByHandle.get(entryHandle);
                DocumentLocation loc = pointer.getLocation();
                PersistedEntry entry = new PersistedEntry(loc.shardId(), loc.entryMetadata().handle(), document);
                ctx.sinks().writePersistedEntry(sink, loc.versionstamp(), entry);
            }
        }

        // Cleanup
        for (PipelineNode child : children()) {
            PipelineNode head = findHeadNode(child);
            DataSink childSink = ctx.sinks().load(head.id());
            if (childSink == null) {
                continue;
            }
            childSink.clear();
        }
    }

    /**
     * Temporary container for aggregating document metadata from multiple sources.
     * <p>
     * During union execution, the same document may appear in multiple child results
     * with different representations (some as persisted entries, others as locations).
     * DocumentPointer accumulates all available metadata for a document, enabling
     * the union logic to choose the most efficient retrieval path.
     */
    private static class DocumentPointer {
        private DocumentLocation location;
        private Versionstamp versionstamp;
        private PersistedEntry persistedEntry;

        public DocumentLocation getLocation() {
            return location;
        }

        public void setLocation(DocumentLocation location) {
            this.location = location;
        }

        public Versionstamp getVersionstamp() {
            return versionstamp;
        }

        public void setVersionstamp(Versionstamp versionstamp) {
            this.versionstamp = versionstamp;
        }

        public PersistedEntry getPersistedEntry() {
            return persistedEntry;
        }

        public void setPersistedEntry(PersistedEntry persistedEntry) {
            this.persistedEntry = persistedEntry;
        }
    }
}
