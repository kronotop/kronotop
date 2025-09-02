package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.IndexDefinition;

import java.nio.ByteBuffer;

public class FullScanNode extends AbstractTransactionAwareNode implements ScanNode {
    private final IndexDefinition index = DefaultIndexDefinition.ID;
    private final ResidualPredicateNode predicate;

    protected FullScanNode(int id, ResidualPredicateNode predicate) {
        super(id);
        this.predicate = predicate;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }

    public ResidualPredicateNode predicate() {
        return predicate;
    }

    @Override
    public void execute(QueryContext ctx, Transaction tr) {
        DirectorySubspace idIndexSubspace = ctx.metadata().indexes().getSubspace(index().selector());
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        FullScanContext fullScanCtx = new FullScanContext(id(), idIndexSubspace, state, ctx.options().isReverse());
        SelectorPair selectors = SelectorCalculator.calculate(fullScanCtx);

        AsyncIterable<KeyValue> indexEntries = tr.getRange(
                selectors.begin(),
                selectors.end(),
                state.getLimit(),
                ctx.options().isReverse()
        );

        int counter = 0;
        for (KeyValue indexEntry : indexEntries) {
            counter++;
            DocumentLocation location = ctx.env().documentRetriever().extractDocumentLocationFromIndexScan(idIndexSubspace, indexEntry);
            Versionstamp versionstamp = location.versionstamp();
            ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.metadata(), location);

            if (predicate.test(document)) {
                ctx.output().appendDocument(id(), versionstamp, document);
            }
            ctx.env().cursorManager().saveFullScanCheckpoint(ctx, id(), versionstamp);
        }
        state.setSelector(selectors);
        state.setExhausted(counter <= 0);
    }
}
