package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.IndexDefinition;

public final class IndexScanNode extends AbstractTransactionAwareNode implements ScanNode<IndexScanPredicate> {
    private final IndexDefinition index;
    private final IndexScanPredicate predicate;

    public IndexScanNode(int id, IndexDefinition index, IndexScanPredicate predicate) {
        super(id);
        this.index = index;
        this.predicate = predicate;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }

    @Override
    public IndexScanPredicate predicate() {
        return predicate;
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        DirectorySubspace indexSubspace = ctx.getMetadata().indexes().getSubspace(index().selector());
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        IndexScanContext indexScanContext = new IndexScanContext(id(), indexSubspace, state, ctx.isReverse(), predicate(), index());
        SelectorPair selectors = ctx.env().selectorCalculator().calculateSelectors(indexScanContext);
        KeySelector beginSelector = selectors.beginSelector();
        KeySelector endSelector = selectors.endSelector();

        AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, state.getLimit(), ctx.isReverse());

        int counter = 0;
        for (KeyValue indexEntry : indexEntries) {
            DocumentLocation location = ctx.env().documentRetriever().extractDocumentLocationFromIndexScan(indexSubspace, indexEntry);
            Versionstamp lastProcessedKey = location.versionstamp();

            // Extract index value for cursor management
            Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
            Object rawIndexValue = indexKeyTuple.get(1);
            BqlValue lastIndexValue = createBqlValueFromIndexValue(rawIndexValue, index().bsonType());
            if (predicate.test(lastIndexValue)) {
                // set output here
                ctx.output().appendLocation(id(), location.entryMetadata().id(), location);
            }
            counter++;
            // set cursor here
            ctx.env().cursorManager().setCursorBoundsForIndexScan(ctx, id(), index(), lastIndexValue, lastProcessedKey);
        }
        state.setExhausted(counter <= 0);
    }
}