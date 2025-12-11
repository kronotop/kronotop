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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSelectionPolicy;

import java.nio.ByteBuffer;

public class FullScanNode extends AbstractScanNode implements ScanNode {
    private final IndexDefinition index = DefaultIndexDefinition.ID;
    private final ResidualPredicateNode predicate;

    protected FullScanNode(int id, ResidualPredicateNode predicate) {
        super(id);
        this.predicate = predicate;
    }

    public ResidualPredicateNode predicate() {
        return predicate;
    }

    @Override
    public void execute(QueryContext ctx, Transaction tr) {
        Index indexRecord = ctx.metadata().indexes().getIndex(index.selector(), IndexSelectionPolicy.READ);
        if (indexRecord == null) {
            throw new IllegalStateException("Index not found for selector: " + index.selector());
        }
        DirectorySubspace idIndexSubspace = indexRecord.subspace();
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        FullScanContext fullScanCtx = new FullScanContext(id(), idIndexSubspace, state, ctx.options().isReverse());
        SelectorPair selectors = SelectorCalculator.calculate(fullScanCtx);

        AsyncIterable<KeyValue> indexEntries = tr.getRange(
                selectors.begin(),
                selectors.end(),
                state.getLimit(),
                ctx.options().isReverse()
        );

        DataSink sink = ctx.sinks().loadOrCreatePersistedEntrySink(id());
        int counter = 0;
        for (KeyValue indexEntry : indexEntries) {
            counter++;
            DocumentLocation location = ctx.env().documentRetriever().extractDocumentLocationFromIndexScan(idIndexSubspace, indexEntry);
            Versionstamp versionstamp = location.versionstamp();
            ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.metadata(), location);

            if (predicate.test(document)) {
                PersistedEntry entry = new PersistedEntry(location.shardId(), location.entryMetadata().handle(), document);
                ctx.sinks().writePersistedEntry(sink, versionstamp, entry);
            }
            ctx.env().cursorManager().saveFullScanCheckpoint(ctx, id(), versionstamp);
        }
        state.setSelector(selectors);
        state.setExhausted(counter <= 0);
    }
}
