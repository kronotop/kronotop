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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.PrimaryIndex;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FullScanNode extends AbstractScanNode implements ScanNode {
    private final SingleFieldIndexDefinition index;
    private final ResidualPredicateNode predicate;
    private boolean collationMismatch;
    private String rejectedIndex;

    protected FullScanNode(int id, SingleFieldIndexDefinition index, ResidualPredicateNode predicate) {
        super(id);
        this.index = index;
        this.predicate = predicate;
    }

    public ResidualPredicateNode predicate() {
        return predicate;
    }

    public boolean isCollationMismatch() {
        return collationMismatch;
    }

    public void setCollationMismatch(boolean collationMismatch) {
        this.collationMismatch = collationMismatch;
    }

    public String getRejectedIndex() {
        return rejectedIndex;
    }

    public void setRejectedIndex(String rejectedIndex) {
        this.rejectedIndex = rejectedIndex;
    }

    @Override
    public SingleFieldIndexDefinition getIndexDefinition() {
        return index;
    }

    @Override
    public void execute(QueryContext ctx, Transaction tr) {
        // Track which field this scan uses for SORTBY optimization (primary index = _id)
        ctx.setScannedIndexField(PrimaryIndex.SELECTOR);

        Index indexRecord = ctx.metadata().indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        if (indexRecord == null) {
            throw new IllegalStateException("Index not found for selector: " + PrimaryIndex.SELECTOR);
        }
        DirectorySubspace idIndexSubspace = indexRecord.subspace();
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        boolean shouldReverse = getShouldReverse(ctx, PrimaryIndex.SELECTOR);
        state.setScanReversed(shouldReverse);
        SortDirection effectiveDirection = getEffectiveSortDirection(ctx, shouldReverse);

        FullScanContext fullScanCtx = new FullScanContext(id(), idIndexSubspace, state, effectiveDirection);
        SelectorPair selectors = SelectorCalculator.calculate(fullScanCtx);

        AsyncIterable<KeyValue> indexEntries = getRange(
                tr, ctx,
                selectors.begin(),
                selectors.end(),
                state.getLimit(),
                shouldReverse
        );

        // Phase 1: Collect all document locations
        List<DocumentLocation> locations = new ArrayList<>();
        for (KeyValue indexEntry : indexEntries) {
            DocumentLocation location = ctx.env().documentRetriever().extractDocumentLocationFromIndexScan(idIndexSubspace, indexEntry);
            locations.add(location);
        }

        state.setSelector(selectors);
        state.setExhausted(locations.isEmpty());

        if (locations.isEmpty()) {
            return;
        }

        // Phase 2: Batch retrieve all documents
        List<ByteBuffer> documents = ctx.env().documentRetriever().retrieveDocuments(locations);

        // Phase 3: Filter and write results
        PersistedEntrySink sink = ctx.sinks().loadOrCreatePersistedEntrySink(id());
        ObjectId lastObjectId = null;
        List<BqlValue> parameters = ctx.getParameters();
        DocumentView view = new DocumentView();

        for (int i = 0; i < documents.size(); i++) {
            ByteBuffer document = documents.get(i);
            DocumentLocation location = locations.get(i);
            ObjectId objectId = location.objectId();

            view.reset(objectId, document);
            if (predicate.test(view, parameters, ctx.env().collatorCache())) {
                PersistedEntry entry = new PersistedEntry(
                        objectId,
                        location.shardId(),
                        location.entryMetadata(),
                        document
                );
                sink.append(entry);
            }
            lastObjectId = objectId;
        }

        // Save checkpoint at the end
        if (lastObjectId != null) {
            ctx.env().cursorManager().saveObjectIdCheckpoint(ctx, id(), lastObjectId, shouldReverse);
        }
    }
}
