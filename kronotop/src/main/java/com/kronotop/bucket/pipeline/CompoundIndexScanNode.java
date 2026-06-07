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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.volume.EntryMetadata;
import org.bson.BsonType;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pipeline scan node that performs a single FoundationDB range scan on a compound index subspace.
 *
 * <p>Partitions filters into an equality prefix and an optional range tail on the last-matched field,
 * then constructs begin/end KeySelectors for a single {@code getRange} call.</p>
 */
public class CompoundIndexScanNode extends AbstractScanNode implements ScanNode {
    private final CompoundIndexDefinition index;
    private final List<CompoundIndexScanFilter> filters;

    public CompoundIndexScanNode(int id, CompoundIndexDefinition index, List<CompoundIndexScanFilter> filters) {
        super(id);
        this.index = index;
        this.filters = filters;
    }

    public CompoundIndexDefinition indexDefinition() {
        return index;
    }

    public List<CompoundIndexScanFilter> filters() {
        return filters;
    }

    @Override
    public CompoundIndexDefinition getIndexDefinition() {
        return index;
    }

    @Override
    public void execute(QueryContext ctx, Transaction tr) {
        // Set scannedIndexField for SORTBY optimization if the compound index provides natural order
        String sortByField = ctx.options().getSortByField();
        if (sortByField != null) {
            String naturalSortField = determineNaturalSortField(sortByField);
            if (naturalSortField != null) {
                ctx.setScannedIndexField(naturalSortField);
            }
        }

        CompoundIndex compoundIndex = ctx.metadata().compoundIndexes().getIndexByName(index.name(), IndexSelectionPolicy.READ);
        if (compoundIndex == null) {
            throw new IllegalStateException("Compound index not found: " + index.name());
        }
        DirectorySubspace indexSubspace = compoundIndex.subspace();
        ExecutionState state = ctx.getOrCreateExecutionState(id());
        List<BqlValue> parameters = ctx.getParameters();

        Collation collation = IndexMaintainer.resolveCollation(index, ctx.metadata());
        CollatorCache collatorCache = ctx.env().collatorCache();

        SelectorPair selectors;
        if (!state.isEmpty()) {
            // Continuation: use pre-computed selectors saved from the previous page
            selectors = state.getSelector();
        } else {
            selectors = buildFreshSelectors(indexSubspace, parameters, collation, collatorCache);
        }

        boolean shouldReverse = getShouldReverseForCompoundIndex(ctx);
        state.setScanReversed(shouldReverse);
        AsyncIterable<KeyValue> indexEntries = getRange(tr, ctx, selectors.begin(), selectors.end(), state.getLimit(), shouldReverse);

        DocumentLocationSink sink = ctx.sinks().loadOrCreateDocumentLocationSink(id());
        ObjectId lastObjectId = null;
        byte[] lastKeyBytes = null;
        int counter = 0;
        int fieldCount = index.fields().size();

        for (KeyValue kv : indexEntries) {
            Tuple keyTuple = indexSubspace.unpack(kv.getKey());
            // Key layout: (ENTRIES, val1, ..., valN, ObjectId)
            byte[] objectIdBytes = keyTuple.getBytes(fieldCount + 1);
            ObjectId objectId = new ObjectId(objectIdBytes);

            IndexEntry indexEntryData = IndexEntry.decode(kv.getValue());
            int shardId = indexEntryData.shardId();
            EntryMetadata entryMetadata = EntryMetadata.decode(indexEntryData.entryMetadata());

            DocumentLocation location = new DocumentLocation(objectId, shardId, entryMetadata);
            sink.append(location);
            lastObjectId = objectId;
            lastKeyBytes = kv.getKey();
            counter++;
        }

        if (lastObjectId != null) {
            // Save ObjectId checkpoint (makes state.isEmpty() return false for next call)
            ctx.env().cursorManager().saveObjectIdCheckpoint(ctx, id(), lastObjectId, shouldReverse);
            // Pre-compute continuation selectors for the next page
            SelectorPair continuationSelectors;
            if (shouldReverse) {
                continuationSelectors = new SelectorPair(
                        selectors.begin(),
                        KeySelector.firstGreaterOrEqual(lastKeyBytes)
                );
            } else {
                continuationSelectors = new SelectorPair(
                        KeySelector.firstGreaterThan(lastKeyBytes),
                        selectors.end()
                );
            }
            state.setSelector(continuationSelectors);
        } else {
            state.setSelector(selectors);
        }
        state.setExhausted(counter <= 0);
    }

    private SelectorPair buildFreshSelectors(DirectorySubspace indexSubspace, List<BqlValue> parameters,
                                             Collation collation, CollatorCache collatorCache) {
        // Partition filters into EQ prefix and range filters
        List<Object> eqValues = new ArrayList<>();
        List<CompoundIndexScanFilter> rangeFilters = new ArrayList<>();

        for (CompoundIndexScanFilter filter : filters) {
            if (filter.op() == Operator.EQ) {
                BqlValue resolved = filter.operand().resolve(parameters);
                Object value = SelectorCalculator.extractIndexValueFromBqlValue(resolved, filter.bsonType());
                value = IndexMaintainer.applyCollation(value, collation, collatorCache);
                eqValues.add(value);
            } else {
                rangeFilters.add(filter);
            }
        }

        if (rangeFilters.isEmpty()) {
            return buildAllEqSelectors(indexSubspace, eqValues);
        }
        return buildRangeSelectors(indexSubspace, eqValues, rangeFilters, parameters, collation, collatorCache);
    }

    private SelectorPair buildAllEqSelectors(DirectorySubspace indexSubspace, List<Object> eqValues) {
        Object[] items = new Object[eqValues.size() + 1];
        items[0] = IndexSubspaceMagic.ENTRIES.getValue();
        for (int i = 0; i < eqValues.size(); i++) {
            items[i + 1] = eqValues.get(i);
        }
        byte[] prefix = indexSubspace.pack(Tuple.from(items));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        return new SelectorPair(begin, end);
    }

    private SelectorPair buildRangeSelectors(
            DirectorySubspace indexSubspace,
            List<Object> eqValues,
            List<CompoundIndexScanFilter> rangeFilters,
            List<BqlValue> parameters,
            Collation collation,
            CollatorCache collatorCache) {

        // Build EQ prefix
        Object[] eqItems = new Object[eqValues.size() + 1];
        eqItems[0] = IndexSubspaceMagic.ENTRIES.getValue();
        for (int i = 0; i < eqValues.size(); i++) {
            eqItems[i + 1] = eqValues.get(i);
        }
        byte[] eqPrefix = indexSubspace.pack(Tuple.from(eqItems));

        // Find lower bound (GT/GTE) and upper bound (LT/LTE)
        CompoundIndexScanFilter lowerFilter = null;
        CompoundIndexScanFilter upperFilter = null;
        for (CompoundIndexScanFilter rf : rangeFilters) {
            if (rf.op() == Operator.GT || rf.op() == Operator.GTE) {
                lowerFilter = rf;
            } else if (rf.op() == Operator.LT || rf.op() == Operator.LTE) {
                upperFilter = rf;
            }
        }

        KeySelector begin;
        if (lowerFilter != null) {
            byte[] fullKey = packEqPrefixPlusValue(indexSubspace, eqValues, lowerFilter, parameters, collation, collatorCache);
            begin = (lowerFilter.op() == Operator.GT)
                    ? KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(fullKey))
                    : KeySelector.firstGreaterOrEqual(fullKey);
        } else {
            begin = KeySelector.firstGreaterOrEqual(eqPrefix);
        }

        KeySelector end;
        if (upperFilter != null) {
            byte[] fullKey = packEqPrefixPlusValue(indexSubspace, eqValues, upperFilter, parameters, collation, collatorCache);
            end = (upperFilter.op() == Operator.LT)
                    ? KeySelector.firstGreaterOrEqual(fullKey)
                    : KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(fullKey));
        } else {
            end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(eqPrefix));
        }

        return new SelectorPair(begin, end);
    }

    private boolean getShouldReverseForCompoundIndex(QueryContext ctx) {
        if (!ctx.options().isReverse()) {
            return false;
        }
        String sortByField = ctx.options().getSortByField();
        if (sortByField == null) {
            return true;
        }

        // If sortByField is an EQ filter field, all values are identical — reversing is pointless
        if (isEqFilterField(sortByField)) {
            return false;
        }

        // If sortByField is the natural sort field (first non-EQ field), reverse for DESC
        String naturalSortField = determineNaturalSortField(sortByField);
        return naturalSortField != null;
    }

    /**
     * Determines whether the compound index provides natural sort order for the given field.
     *
     * <p>Walks the index fields in order: fields with EQ filters form an equality prefix
     * (constant values, trivially sorted). The first non-EQ field is naturally sorted by
     * tuple ordering. Returns the field name if sortByField matches any EQ-prefix field
     * or the first non-EQ field; null otherwise.</p>
     */
    private String determineNaturalSortField(String sortByField) {
        // Build filter-op lookup from the 'filters' list.
        Map<String, Operator> filterOps = new HashMap<>();
        for (CompoundIndexScanFilter filter : filters) {
            filterOps.put(filter.selector(), filter.op());
        }

        for (CompoundIndexField field : index.fields()) {
            Operator op = filterOps.get(field.selector());
            if (op == Operator.EQ) {
                // Part of EQ prefix — trivially sorted
                if (field.selector().equals(sortByField)) {
                    return sortByField;
                }
            } else {
                // First non-EQ field — naturally sorted by tuple ordering
                if (field.selector().equals(sortByField)) {
                    return sortByField;
                }
                // Beyond this point, fields are not naturally sorted
                return null;
            }
        }
        return null;
    }

    private boolean isEqFilterField(String field) {
        for (CompoundIndexScanFilter filter : filters) {
            if (filter.selector().equals(field) && filter.op() == Operator.EQ) {
                return true;
            }
        }
        return false;
    }

    private byte[] packEqPrefixPlusValue(
            DirectorySubspace indexSubspace,
            List<Object> eqValues,
            CompoundIndexScanFilter filter,
            List<BqlValue> parameters,
            Collation collation,
            CollatorCache collatorCache) {
        BqlValue resolved = filter.operand().resolve(parameters);
        Object val = SelectorCalculator.extractIndexValueFromBqlValue(resolved, filter.bsonType());
        val = IndexMaintainer.applyCollation(val, collation, collatorCache);
        Object[] items = new Object[eqValues.size() + 2];
        items[0] = IndexSubspaceMagic.ENTRIES.getValue();
        for (int i = 0; i < eqValues.size(); i++) {
            items[i + 1] = eqValues.get(i);
        }
        items[items.length - 1] = val;
        return indexSubspace.pack(Tuple.from(items));
    }

    /**
     * A single filter within a compound index scan.
     */
    public record CompoundIndexScanFilter(String selector, Operator op, Operand operand, BsonType bsonType) {
    }
}
