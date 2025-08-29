package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeySelector;
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
    public void execute(PipelineContext ctx, Transaction tr) {
        DirectorySubspace idIndexSubspace = ctx.getMetadata().indexes().getSubspace(index().selector());
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        PrimaryIndexScanContext indexScanContext = new PrimaryIndexScanContext(id(), idIndexSubspace, state, ctx.isReverse());
        SelectorPair selectors = ctx.env().selectorCalculator().calculateSelectors(indexScanContext);
        KeySelector beginSelector = selectors.beginSelector();
        KeySelector endSelector = selectors.endSelector();

        AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, state.getLimit(), ctx.isReverse());

        int counter = 0;
        for (KeyValue indexEntry : indexEntries) {
            counter++;
            DocumentLocation location = ctx.env().documentRetriever().extractDocumentLocationFromIndexScan(idIndexSubspace, indexEntry);
            Versionstamp versionstamp = location.versionstamp();
            ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.getMetadata(), location);

            if (predicate.test(document)) {
                ctx.output().appendDocument(id(), versionstamp, document);
            }
            ctx.env().cursorManager().savePrimaryIndexCheckpoint(ctx, id(), versionstamp);
        }
        state.setExhausted(counter <= 0);
    }
}
