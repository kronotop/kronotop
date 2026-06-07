/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.bql.ast.BinaryVal;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.index.IndexMaintainer;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.Operator;
import org.bson.types.ObjectId;

import java.util.List;

public class IndexScanNode extends AbstractScanNode implements ScanNode {
    private final SingleFieldIndexDefinition index;
    private final IndexScanPredicate predicate;

    public IndexScanNode(int id, SingleFieldIndexDefinition index, IndexScanPredicate predicate) {
        super(id);
        this.index = index;
        this.predicate = predicate;
    }

    public IndexScanPredicate predicate() {
        return predicate;
    }

    @Override
    public SingleFieldIndexDefinition getIndexDefinition() {
        return index;
    }

    @Override
    public void execute(QueryContext ctx, Transaction tr) {
        SingleFieldScanSetup setup = setupSingleFieldScan(ctx, index);
        List<BqlValue> parameters = setup.parameters();
        Collation collation = IndexMaintainer.resolveCollation(index, ctx.metadata());

        IndexScanContext indexScanCtx = new IndexScanContext(
                id(),
                setup.indexSubspace(),
                setup.state(),
                setup.effectiveDirection(),
                predicate,
                index,
                parameters,
                collation,
                ctx.env().collatorCache()
        );
        SelectorPair selectors = SelectorCalculator.calculate(indexScanCtx);

        AsyncIterable<KeyValue> indexEntries = getRange(
                tr, ctx,
                selectors.begin(),
                selectors.end(),
                setup.state().getLimit(),
                setup.shouldReverse()
        );

        DocumentLocationSink sink = ctx.sinks().loadOrCreateDocumentLocationSink(id());
        BqlValue lastIndexValue = null;
        ObjectId lastObjectId = null;
        int counter = 0;
        IndexScanPredicate effectivePredicate = predicate;
        if (collation != null && predicate.op() == Operator.NE) {
            BqlValue resolved = predicate.operand().resolve(parameters);
            if (resolved instanceof StringVal(String str)) {
                byte[] collationKey = ctx.env().collatorCache()
                        .acquire(collation).getCollationKey(str).toByteArray();
                effectivePredicate = new IndexScanPredicate(
                        predicate.id(), predicate.selector(), predicate.op(),
                        new Operand.Literal(new BinaryVal(collationKey)));
            }
        }
        for (KeyValue indexEntry : indexEntries) {
            IndexScanResult scanResult = ctx.env().documentRetriever().extractFromIndexScanWithValue(setup.indexSubspace(), indexEntry);
            DocumentLocation location = scanResult.location();
            lastObjectId = location.objectId();
            lastIndexValue = createBqlValueFromIndexValue(scanResult.rawIndexValue(), index.bsonType(), collation);
            if (effectivePredicate.test(lastIndexValue, parameters)) {
                location.setCursorIndexValue(lastIndexValue);
                sink.append(location);
            }
            counter++;
        }
        if (lastObjectId != null) {
            ctx.env().cursorManager().saveIndexScanCheckpoint(ctx, id(), lastIndexValue, lastObjectId, setup.shouldReverse());
        }
        setup.state().setSelector(selectors);
        setup.state().setExhausted(counter <= 0);
    }
}
