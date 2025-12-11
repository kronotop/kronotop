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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSelectionPolicy;

public class RangeScanNode extends AbstractScanNode implements ScanNode {
    private final IndexDefinition index;
    private final RangeScanPredicate predicate;

    protected RangeScanNode(int id, IndexDefinition index, RangeScanPredicate predicate) {
        super(id);
        this.index = index;
        this.predicate = predicate;
    }

    public RangeScanPredicate predicate() {
        return predicate;
    }

    @Override
    public void execute(QueryContext ctx, Transaction tr) {
        Index indexRecord = ctx.metadata().indexes().getIndex(index.selector(), IndexSelectionPolicy.READ);
        if (indexRecord == null) {
            throw new IllegalStateException("Index not found for selector: " + index.selector());
        }
        DirectorySubspace indexSubspace = indexRecord.subspace();
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        RangeScanContext rangeScanCtx = new RangeScanContext(
                id(),
                indexSubspace,
                state,
                ctx.options().isReverse(),
                predicate
        );
        SelectorPair selectors = SelectorCalculator.calculate(rangeScanCtx);

        AsyncIterable<KeyValue> indexEntries = tr.getRange(selectors.begin(), selectors.end(), state.getLimit(), ctx.options().isReverse());

        DataSink sink = ctx.sinks().loadOrCreateDocumentLocationSink(id());
        int counter = 0;
        for (KeyValue indexEntry : indexEntries) {
            DocumentLocation location = ctx.env().documentRetriever().extractDocumentLocationFromIndexScan(indexSubspace, indexEntry);
            Versionstamp versionstamp = location.versionstamp();

            Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
            Object rawIndexValue = indexKeyTuple.get(1);
            BqlValue indexValue = createBqlValueFromIndexValue(rawIndexValue, index.bsonType());
            ctx.sinks().writeDocumentLocation(sink, location.entryMetadata().handle(), location);
            ctx.env().cursorManager().saveIndexScanCheckpoint(ctx, id(), indexValue, versionstamp);
            counter++;
        }
        state.setSelector(selectors);
        state.setExhausted(counter <= 0);
    }

}
