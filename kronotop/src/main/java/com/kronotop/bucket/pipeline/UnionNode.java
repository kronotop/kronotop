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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.bson.types.ObjectId;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.ByteBuffer;
import java.util.*;

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
 * <p>
 * When results exceed the requested limit, excess entries are not buffered. Instead,
 * affected children's cursor checkpoints are restored to their pre-execution state,
 * and the children re-scan on the next ADVANCE call. Cross-advance deduplication via
 * {@code returnedHandles} ensures no duplicates.
 *
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
     * {@code false} if at least one child has more results to produce
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
     * Collects results from all child nodes into bitmaps and a result map.
     */
    private ChildCollectionResult collectChildResults(QueryContext ctx) {
        Map<Long, DocumentPointer> result = new LinkedHashMap<>();
        Roaring64Bitmap[] bitmaps = new Roaring64Bitmap[children().size()];

        for (int index = 0; index < children().size(); index++) {
            PipelineNode child = children().get(index);
            PipelineNode head = findHeadNode(child);
            DataSink childDataSink = ctx.sinks().load(head.id());
            if (childDataSink == null) {
                throw new IllegalStateException("No data sink found for " + child);
            }

            Roaring64Bitmap bitmap = new Roaring64Bitmap();
            switch (childDataSink) {
                case PersistedEntrySink persistedEntrySink -> {
                    for (PersistedEntry entry : persistedEntrySink.entries()) {
                        bitmap.add(entry.entryMetadata().handle());
                        result.compute(entry.entryMetadata().handle(), (ignored, documentPointer) -> {
                            if (documentPointer == null) {
                                DocumentPointer pointer = new DocumentPointer();
                                pointer.setPersistedEntry(entry);
                                return pointer;
                            }
                            documentPointer.setPersistedEntry(entry);
                            return documentPointer;
                        });
                    }
                }
                case DocumentLocationSink documentLocationSink -> {
                    for (DocumentLocation location : documentLocationSink.entries()) {
                        bitmap.add(location.entryMetadata().handle());
                        result.compute(location.entryMetadata().handle(), (ignored, documentPointer) -> {
                            if (documentPointer == null) {
                                DocumentPointer pointer = new DocumentPointer();
                                pointer.setLocation(location);
                                return pointer;
                            }
                            documentPointer.setLocation(location);
                            return documentPointer;
                        });
                    }
                }
            }
            bitmaps[index] = bitmap;
        }

        return new ChildCollectionResult(result, bitmaps);
    }

    /**
     * Applies deduplication filter to remove already-returned entries.
     * <p>
     * For update operations, deduplication uses ObjectId (stable across updates) instead of
     * entry handles (which change when documents are rewritten to new storage locations).
     *
     * @return filtered union bitmap
     */
    private Roaring64Bitmap applyDeduplicationFilter(QueryContext ctx, ExecutionState state, ChildCollectionResult collected) {
        Roaring64Bitmap union = orAll(collected.bitmaps());

        if (ctx.options().update() != null) {
            // Update operations: use ObjectId-based dedup because handles change after update
            Set<ObjectId> returnedObjectIds = state.getReturnedObjectIds();
            if (returnedObjectIds == null) {
                returnedObjectIds = new HashSet<>();
                state.setReturnedObjectIds(returnedObjectIds);
            }
            Roaring64Bitmap toRemove = new Roaring64Bitmap();
            LongIterator it = union.getLongIterator();
            while (it.hasNext()) {
                long handle = it.next();
                DocumentPointer pointer = collected.result().get(handle);
                ObjectId objectId = pointer.getPersistedEntry() != null
                        ? pointer.getPersistedEntry().objectId()
                        : pointer.getLocation().objectId();
                if (returnedObjectIds.contains(objectId)) {
                    toRemove.add(handle);
                }
            }
            union.andNot(toRemove);
        } else {
            // Query/delete operations: use handle-based dedup (handles are stable)
            Roaring64Bitmap returnedHandles = state.getReturnedHandles();
            if (returnedHandles == null) {
                returnedHandles = new Roaring64Bitmap();
                state.setReturnedHandles(returnedHandles);
            }
            union.andNot(returnedHandles);
        }

        // Remove filtered handles from the result map too
        collected.result().keySet().removeIf(handle -> !union.contains(handle));

        return union;
    }

    /**
     * Writes kept results to the sink and rewinds children that produced excess entries.
     * <p>
     * Splits the union bitmap into kept (within capacity) and excess handles. Documents
     * are retrieved only for kept handles. Children contributing excess entries have their
     * cursor checkpoints restored to pre-execution state so they re-scan on the next
     * ADVANCE call.
     */
    private void writeResultsAndRewindChildren(QueryContext ctx, ExecutionState state, PersistedEntrySink sink,
                                               Roaring64Bitmap union, Map<Long, DocumentPointer> result,
                                               Roaring64Bitmap[] childBitmaps) {
        boolean needsInMemorySort = ctx.options().getSortByField() != null;
        int remainingCapacity = needsInMemorySort ? Integer.MAX_VALUE : ctx.options().limit() - sink.size();

        // Split handles into kept vs excess
        Roaring64Bitmap keptHandles = new Roaring64Bitmap();
        Roaring64Bitmap excessHandles = new Roaring64Bitmap();

        LongIterator it = union.getLongIterator();
        while (it.hasNext()) {
            long handle = it.next();
            if (remainingCapacity > 0) {
                keptHandles.add(handle);
                remainingCapacity--;
            } else {
                excessHandles.add(handle);
            }
        }

        // Retrieve documents only for kept handles
        List<Long> handlesNeedingRetrieval = new ArrayList<>();
        List<DocumentLocation> locationsNeedingRetrieval = new ArrayList<>();

        LongIterator keptIt = keptHandles.getLongIterator();
        while (keptIt.hasNext()) {
            long handle = keptIt.next();
            DocumentPointer pointer = result.get(handle);
            if (pointer.getPersistedEntry() == null) {
                handlesNeedingRetrieval.add(handle);
                locationsNeedingRetrieval.add(pointer.getLocation());
            }
        }

        Long2ObjectMap<ByteBuffer> retrievedByHandle = new Long2ObjectOpenHashMap<>();
        if (!locationsNeedingRetrieval.isEmpty()) {
            List<ByteBuffer> docs = ctx.env().documentRetriever().retrieveDocuments(locationsNeedingRetrieval);
            for (int i = 0; i < handlesNeedingRetrieval.size(); i++) {
                retrievedByHandle.put((long) handlesNeedingRetrieval.get(i), docs.get(i));
            }
        }

        // Write kept entries to sink
        keptIt = keptHandles.getLongIterator();
        while (keptIt.hasNext()) {
            long handle = keptIt.next();
            DocumentPointer pointer = result.get(handle);

            PersistedEntry entry;
            if (pointer.getPersistedEntry() != null) {
                entry = pointer.getPersistedEntry();
            } else {
                ByteBuffer document = retrievedByHandle.get(handle);
                DocumentLocation loc = pointer.getLocation();
                entry = new PersistedEntry(loc.objectId(), loc.shardId(), loc.entryMetadata(), document);
            }
            sink.append(entry);
        }

        // Track kept entries for cross-advance deduplication
        if (ctx.options().update() != null) {
            // Update operations: track ObjectIds (stable across updates)
            Set<ObjectId> returnedObjectIds = state.getReturnedObjectIds();
            LongIterator trackIt = keptHandles.getLongIterator();
            while (trackIt.hasNext()) {
                long handle = trackIt.next();
                DocumentPointer pointer = result.get(handle);
                ObjectId objectId = pointer.getPersistedEntry() != null
                        ? pointer.getPersistedEntry().objectId()
                        : pointer.getLocation().objectId();
                returnedObjectIds.add(objectId);
            }
            state.setReturnedObjectIds(returnedObjectIds);
        } else {
            // Query/delete operations: track handles (stable when documents don't change)
            Roaring64Bitmap returnedHandles = state.getReturnedHandles();
            returnedHandles.or(keptHandles);
            state.setReturnedHandles(returnedHandles);
        }

        // Rewind children with excess entries (skip when sorting)
        if (!needsInMemorySort && !excessHandles.isEmpty()) {
            rewindChildrenWithExcess(ctx, state, childBitmaps, excessHandles);
        }
    }

    /**
     * Restores pre-execution cursor checkpoints for children that contributed excess entries.
     * <p>
     * On the next ADVANCE call, these children re-scan from their restored positions.
     * Previously returned entries are filtered by deduplication before document retrieval,
     * so only excess entries are actually processed.
     */
    private void rewindChildrenWithExcess(QueryContext ctx, ExecutionState unionState,
                                          Roaring64Bitmap[] childBitmaps, Roaring64Bitmap excessHandles) {
        Map<Integer, ExecutionState.SavedCursorCheckpoint> saved = unionState.getSavedChildCheckpoints();
        if (saved == null) {
            return;
        }

        for (int i = 0; i < children().size(); i++) {
            PipelineNode child = children().get(i);
            Roaring64Bitmap childBitmap = childBitmaps[i];
            if (childBitmap == null) {
                continue;
            }

            // Check if this child contributed any excess entries
            Roaring64Bitmap childExcess = Roaring64Bitmap.and(childBitmap, excessHandles);
            if (childExcess.isEmpty()) {
                continue;
            }

            // Restore this child's pre-execution checkpoint
            ExecutionState.SavedCursorCheckpoint checkpoint = saved.get(child.id());
            if (checkpoint != null) {
                ExecutionState childState = ctx.getOrCreateExecutionState(child.id());
                childState.setLower(checkpoint.lower());
                childState.setUpper(checkpoint.upper());
                childState.setSelector(checkpoint.selector());
                childState.setExhausted(false);
            }
        }
    }

    /**
     * Clears all child sinks to release memory.
     */
    private void clearChildSinks(QueryContext ctx) {
        for (PipelineNode child : children()) {
            PipelineNode head = findHeadNode(child);
            DataSink childSink = ctx.sinks().load(head.id());
            if (childSink != null) {
                childSink.clear();
            }
        }
    }

    /**
     * Executes the union operation by merging results from all child nodes.
     * <p>
     * Execution proceeds in four phases:
     * <ol>
     *   <li><b>Exhaustion Check</b>: Exits early if all children have completed.</li>
     *   <li><b>Collection</b>: Gathers entry handles and document metadata from child sinks into bitmaps.</li>
     *   <li><b>Deduplication</b>: Filters already-returned handles to prevent cross-advance duplicates.</li>
     *   <li><b>Materialization and Rewind</b>: Retrieves documents for kept entries, writes results,
     *       and restores cursor checkpoints for children that produced excess entries.</li>
     * </ol>
     *
     * @param ctx the query context providing access to sinks, document retriever, and execution state
     * @throws IllegalStateException if a child node has no associated data sink
     */
    @Override
    public void execute(QueryContext ctx) {
        ExecutionState state = ctx.getOrCreateExecutionState(id());
        PersistedEntrySink sink = ctx.sinks().loadOrCreatePersistedEntrySink(id());

        // Phase 1: Check if all children are exhausted
        if (checkAndPropagateExhaustion(ctx)) {
            return;
        }

        // Phase 2: Collect results from all child nodes
        ChildCollectionResult collected = collectChildResults(ctx);

        // Phase 3: Apply cross-advance deduplication filter
        Roaring64Bitmap union = applyDeduplicationFilter(ctx, state, collected);

        // Phase 4: Write results and rewind children for excess
        writeResultsAndRewindChildren(ctx, state, sink, union, collected.result(), collected.bitmaps());

        // Cleanup: Release memory from child sinks
        clearChildSinks(ctx);
    }

    /**
     * Result of collecting child node outputs during union execution.
     *
     * @param result  map from entry handle to document pointer
     * @param bitmaps per-child bitmaps of entry handles
     */
    private record ChildCollectionResult(
            Map<Long, DocumentPointer> result,
            Roaring64Bitmap[] bitmaps
    ) {
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
        private PersistedEntry persistedEntry;

        public DocumentLocation getLocation() {
            return location;
        }

        public void setLocation(DocumentLocation location) {
            this.location = location;
        }

        public PersistedEntry getPersistedEntry() {
            return persistedEntry;
        }

        public void setPersistedEntry(PersistedEntry persistedEntry) {
            this.persistedEntry = persistedEntry;
        }
    }
}
