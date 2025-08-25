package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.IndexDefinition;

import java.util.Arrays;
import java.util.List;

public final class IndexScanNode extends AbstractScanNode {
    public IndexScanNode(int id, IndexDefinition index, List<IndexScanPredicate> predicates) {
        super(id, index, predicates);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        System.out.printf("IndexScanNode ==> %d, %s, %s%n", id(), index(), predicates());
        IndexScanPredicate predicate = predicates().getFirst();
        DirectorySubspace subspace = ctx.getMetadata().indexes().getSubspace(index().selector());
        Cursor cursor = ctx.getCursor(id());

        // Continue scanning until we find results or exhaust the index
        while (true) {
            IndexScanContext indexScanContext = new IndexScanContext(id(), subspace, cursor, ctx.isReverse(), predicate, index());
            SelectorPair selectors = ctx.dependencies().selectorCalculator().calculateSelectors(indexScanContext);
            KeySelector beginSelector = selectors.beginSelector();
            KeySelector endSelector = selectors.endSelector();

            AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, ctx.limit(), ctx.isReverse());
            Versionstamp lastProcessedKey = null;
            BqlValue lastIndexValue = null;
            boolean hasIndexEntries = false;

            for (KeyValue indexEntry : indexEntries) {
                System.out.println(Arrays.toString(indexEntry.getKey()));
                System.out.println(Arrays.toString(indexEntry.getValue()));
            }
            break;
        }
    }
}