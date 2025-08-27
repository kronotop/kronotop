package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.IndexDefinition;

import java.nio.ByteBuffer;

public class FullScanNode extends AbstractTransactionAwareNode implements ScanNode {
    private final IndexDefinition index = DefaultIndexDefinition.ID;
    private final FullScanPredicate predicate;

    protected FullScanNode(int id, FullScanPredicate predicate) {
        super(id);
        this.predicate = predicate;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        DirectorySubspace idIndexSubspace = ctx.getMetadata().indexes().getSubspace(index().selector());
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        PrimaryIndexScanContext indexScanContext = new PrimaryIndexScanContext(id(), idIndexSubspace, state, ctx.isReverse(), predicate);
        SelectorPair selectors = ctx.env().selectorCalculator().calculateSelectors(indexScanContext);
        KeySelector beginSelector = selectors.beginSelector();
        KeySelector endSelector = selectors.endSelector();

        AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, state.getLimit(), ctx.isReverse());

        int counter = 0;
        for (KeyValue indexEntry : indexEntries) {
            DocumentLocation location = ctx.env().documentRetriever().extractDocumentLocationFromIndexScan(idIndexSubspace, indexEntry);
            Versionstamp lastProcessedKey = location.versionstamp();
            ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.getMetadata(), location);
            if (predicate.test(lastProcessedKey, document)) {
                System.out.println(BSONUtil.fromBson(document.array()).toJson());
            }
        }
    }
}
