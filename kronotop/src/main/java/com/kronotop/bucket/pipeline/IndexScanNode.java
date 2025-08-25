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

import java.util.List;

public final class IndexScanNode extends AbstractScanNode {
    public IndexScanNode(int id, IndexDefinition index, List<IndexScanPredicate> predicates) {
        super(id, index, predicates);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        IndexScanPredicate predicate = predicates().getFirst();
        DirectorySubspace indexSubspace = ctx.getMetadata().indexes().getSubspace(index().selector());
        Cursor cursor = ctx.getOrCreateCursor(id());

        IndexScanContext indexScanContext = new IndexScanContext(id(), indexSubspace, cursor, ctx.isReverse(), predicate, index());
        SelectorPair selectors = ctx.dep().selectorCalculator().calculateSelectors(indexScanContext);
        KeySelector beginSelector = selectors.beginSelector();
        KeySelector endSelector = selectors.endSelector();

        AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, ctx.limit(), ctx.isReverse());
        Versionstamp lastProcessedKey = null;
        BqlValue lastIndexValue = null;

        // 1- Test predicate - [OK]
        // 2- Collect DocumentLocation instances [OK]
        // 3- Set output
        for (KeyValue indexEntry : indexEntries) {
            DocumentLocation location = ctx.dep().documentRetriever().extractDocumentLocationFromIndexScan(indexSubspace, indexEntry);
            lastProcessedKey = location.versionstamp();

            // Extract index value for cursor management
            Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
            Object rawIndexValue = indexKeyTuple.get(1);
            lastIndexValue = createBqlValueFromIndexValue(rawIndexValue, index().bsonType());
            if (predicate.canEvaluate() && predicate.test(lastIndexValue)) {
                // set output here
                ctx.output().appendLocation(id(), location.entryMetadata().id(), location);
            }
            // set cursor here
        }
    }
}