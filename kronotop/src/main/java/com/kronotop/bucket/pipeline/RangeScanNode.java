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

public class RangeScanNode extends AbstractTransactionAwareNode implements ScanNode {
    private final IndexDefinition index;
    private final RangeScanPredicate predicate;

    protected RangeScanNode(int id, IndexDefinition index, RangeScanPredicate predicate) {
        super(id);
        this.index = index;
        this.predicate = predicate;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        DirectorySubspace indexSubspace = ctx.getMetadata().indexes().getSubspace(index().selector());
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        RangeScanContext indexScanContext = new RangeScanContext(
                id(),
                indexSubspace,
                state,
                ctx.isReverse(),
                predicate,
                index()
        );
        SelectorPair selectors = ctx.env().selectorCalculator().calculateSelectors(indexScanContext);
        KeySelector beginSelector = selectors.beginSelector();
        KeySelector endSelector = selectors.endSelector();

        AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, state.getLimit(), ctx.isReverse());

        int counter = 0;
        for (KeyValue indexEntry : indexEntries) {
            DocumentLocation location = ctx.env().documentRetriever().extractDocumentLocationFromIndexScan(indexSubspace, indexEntry);
            Versionstamp versionstamp = location.versionstamp();

            Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
            Object rawIndexValue = indexKeyTuple.get(1);
            BqlValue indexValue = createBqlValueFromIndexValue(rawIndexValue, index().bsonType());
            ctx.output().appendLocation(id(), location.entryMetadata().id(), location);
            ctx.env().cursorManager().saveSecondaryIndexCheckpoint(ctx, id(), indexValue, versionstamp);
            counter++;
        }
        state.setExhausted(counter <= 0);
    }

}
