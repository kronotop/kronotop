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
import java.util.List;

public class FullScanNode extends AbstractTransactionAwareNode implements ScanNode {
    private final IndexDefinition index = DefaultIndexDefinition.ID;
    private final List<ResidualPredicate> predicates;
    private MatchingRule matchingRule = MatchingRule.ALL;

    protected FullScanNode(int id, List<ResidualPredicate> predicates) {
        super(id);
        this.predicates = predicates;
    }

    protected FullScanNode(int id, List<ResidualPredicate> predicates, MatchingRule matchingRule) {
        super(id);
        this.predicates = predicates;
        this.matchingRule = matchingRule;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }

    public List<ResidualPredicate> predicates() {
        return predicates;
    }

    private boolean applyALLMatchingRule(Versionstamp versionstamp, ByteBuffer document) {
        boolean matched = true;
        for (ResidualPredicate predicate : predicates) {
            if (!predicate.test(versionstamp, document)) {
                matched = false;
                break;
            }
        }
        return matched;
    }

    private boolean applyANYMatchingRule(Versionstamp versionstamp, ByteBuffer document) {
        for (ResidualPredicate predicate : predicates) {
            if (predicate.test(versionstamp, document)) {
                return true;
            }
        }
        return false;
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

            boolean append = false;
            if (matchingRule.equals(MatchingRule.ALL)) {
                append = applyALLMatchingRule(versionstamp, document);
            } else if (matchingRule.equals(MatchingRule.ANY)) {
                append = applyANYMatchingRule(versionstamp, document);
            }

            if (append) {
                ctx.output().appendDocument(id(), versionstamp, document);
            }
            ctx.env().cursorManager().savePrimaryIndexCheckpoint(ctx, id(), versionstamp);
        }
        state.setExhausted(counter <= 0);
    }
}
