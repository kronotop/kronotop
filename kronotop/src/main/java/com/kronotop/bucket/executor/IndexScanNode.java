package com.kronotop.bucket.executor;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.IndexDefinition;

import java.util.List;

public final class IndexScanNode extends AbstractScanNode {
    public IndexScanNode(int id, IndexDefinition index, List<Predicate> predicates) {
        super(id, index, predicates);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        System.out.printf("IndexScanNode ==> %d, %s, %s%n", id(), index(), predicates());

        DirectorySubspace indexSubspace = ctx.config().getMetadata().indexes().getSubspace(index().selector());
        if (indexSubspace == null) {
            throw new IllegalStateException("Index subspace not found for selector: " + index().selector());
        }

        while(true) {
            CursorState cursor = ctx.getCursor(id());
            Bounds bounds = cursor != null ? cursor.bounds() : null;
            // Calculate selectors using unified SelectorCalculator
            FilterScanContext context = new FilterScanContext(indexSubspace, ctx.config(), bounds, null, index());
            SelectorPair selectors = ctx.selectorCalculator().calculateSelectors(context);
            KeySelector beginSelector = selectors.beginSelector();
            KeySelector endSelector = selectors.endSelector();

            AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, ctx.config().limit(), ctx.config().isReverse());
            Versionstamp lastProcessedKey = null;
            BqlValue lastIndexValue = null;
            boolean hasIndexEntries = false;
        }
    }
}