package com.kronotop.bucket.executor;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;

import java.nio.ByteBuffer;
import java.util.List;

public final class IndexScanNode extends AbstractScanNode {
    private final PhysicalIndexScan plan;

    public IndexScanNode(PhysicalIndexScan physicalNode, List<PipelineNode> children) {
        super(children);
        this.plan = physicalNode;
    }

    private void applyPhysicalFilter(PipelineContext ctx, int nodeId, PhysicalFilter filter, DocumentRetriever.DocumentLocation location) {
        try {
            ByteBuffer document = ctx.documentRetriever().retrieveDocument(ctx.config().getMetadata(), location);

            // Apply the filter
            ByteBuffer filteredDocument;
            if (DefaultIndexDefinition.ID.selector().equals(filter.selector())) {
                // Skip filter evaluation for _id filters since index scan already filtered
                filteredDocument = document;
            } else if (filter.op() == Operator.NE) {
                // Only apply filter evaluation for NE operator - index scan handles all other operators
                filteredDocument = ctx.filterEvaluator().applyPhysicalFilter(filter, document);
            } else {
                // For non-NE operators, index scan already performed filtering
                filteredDocument = document;
            }
            if (filteredDocument != null) {
                ctx.getResults(nodeId).put(location.documentId(), filteredDocument);
            }
        } catch (Exception e) {
            throw new KronotopException(e);
        }
    }


    @Override
    protected void run(Transaction tr, PipelineContext ctx) {
        if (!(plan.node() instanceof PhysicalFilter filter)) {
            throw new IllegalArgumentException("PhysicalNode must be a PhysicalFilter instance");
        }

        // Validate that the operand type matches the index type
        validateIndexOperandType(plan.index(), filter.operand());

        DirectorySubspace indexSubspace = ctx.getMetadata().indexes().getSubspace(filter.selector());
        if (indexSubspace == null) {
            throw new IllegalStateException("Index subspace not found for selector: " + filter.selector());
        }

        while (true) {
            Bounds bounds = ctx.config().cursor().getBounds(plan.id());

            FilterScanContext context = new FilterScanContext(indexSubspace, ctx.config(), bounds, filter, plan.index());
            SelectorPair selectors = ctx.selectorCalculator().calculateSelectors(context);
            KeySelector beginSelector = selectors.beginSelector();
            KeySelector endSelector = selectors.endSelector();

            AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, ctx.config().limit(), ctx.config().isReverse());
            Versionstamp lastProcessedKey = null;
            BqlValue lastIndexValue = null;
            boolean hasIndexEntries = false;
            for (KeyValue indexEntry : indexEntries) {
                hasIndexEntries = true;
                DocumentRetriever.DocumentLocation location = ctx.documentRetriever().extractDocumentLocationFromIndexScan(indexSubspace, indexEntry);
                lastProcessedKey = location.documentId();

                // Extract index value for cursor management
                Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
                Object rawIndexValue = indexKeyTuple.get(1);
                lastIndexValue = ctx.cursorManager().createBqlValueFromIndexValue(rawIndexValue, plan.index().bsonType());

                applyPhysicalFilter(ctx, plan.id(), filter, location);

                // Stop if we've reached the limit
                if (ctx.getResults(plan.id()).size() >= ctx.config().limit()) {
                    break;
                }
            }

            // Handle cursor advancement
            if (!ctx.getResults(plan.id()).isEmpty()) {
                // Found results - set cursor based on last processed values
                if (lastProcessedKey != null && lastIndexValue != null) {
                    ctx.cursorManager().setCursorBoundsForIndexScan(ctx.config(), plan.id(), plan.index(), lastIndexValue, lastProcessedKey);
                }
                break;
            } else if (hasIndexEntries && lastProcessedKey != null) {
                // No results but processed entries - advance cursor and continue
                if (lastIndexValue != null) {
                    ctx.cursorManager().setCursorBoundsForIndexScan(ctx.config(), plan.id(), plan.index(), lastIndexValue, lastProcessedKey);
                }
                continue;
            }
            // No more entries
            break;
        }
    }
}