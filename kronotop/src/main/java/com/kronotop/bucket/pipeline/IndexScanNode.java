package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.IndexDefinition;

public class IndexScanNode extends AbstractScanNode implements ScanNode {
    private final IndexDefinition index;
    private final IndexScanPredicate predicate;

    public IndexScanNode(int id, IndexDefinition index, IndexScanPredicate predicate) {
        super(id);
        this.index = index;
        this.predicate = predicate;
    }

    @Override
    public void execute(QueryContext ctx, Transaction tr) {
        DirectorySubspace indexSubspace = ctx.metadata().indexes().getSubspace(index.selector());
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        IndexScanContext indexScanCtx = new IndexScanContext(
                id(),
                indexSubspace,
                state,
                ctx.options().isReverse(),
                predicate,
                index
        );
        SelectorPair selectors = SelectorCalculator.calculate(indexScanCtx);

        AsyncIterable<KeyValue> indexEntries = tr.getRange(
                selectors.begin(),
                selectors.end(),
                state.getLimit(),
                ctx.options().isReverse()
        );

        DataSink sink = ctx.sinks().loadOrCreateDocumentLocationSink(id());
        int counter = 0;
        for (KeyValue indexEntry : indexEntries) {
            DocumentLocation location = ctx.env().documentRetriever().extractDocumentLocationFromIndexScan(indexSubspace, indexEntry);
            Versionstamp versionstamp = location.versionstamp();

            Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
            Object rawIndexValue = indexKeyTuple.get(1);
            BqlValue indexValue = createBqlValueFromIndexValue(rawIndexValue, index.bsonType());
            if (predicate.test(indexValue)) {
                ctx.sinks().writeDocumentLocation(sink, location.entryMetadata().id(), location);
            }
            counter++;
            ctx.env().cursorManager().saveIndexScanCheckpoint(ctx, id(), indexValue, versionstamp);
        }
        state.setSelector(selectors);
        state.setExhausted(counter <= 0);
    }
}