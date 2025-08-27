/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.planner.Operator;

public class SelectorCalculator {
    private final IndexUtils indexUtils;
    private final CursorManager cursorManager;

    /**
     * Creates a new SelectorCalculator with the required dependencies.
     *
     * @param indexUtils    utility class for index operations and key construction
     * @param cursorManager manages cursor state and pagination positioning
     */
    SelectorCalculator(IndexUtils indexUtils, CursorManager cursorManager) {
        this.indexUtils = indexUtils;
        this.cursorManager = cursorManager;
    }

    SelectorPair calculateSelectors(ScanContext context) {
        return switch (context) {
            case SecondaryIndexScanContext ctx -> calculateIndexScanSelectors(ctx);
            case PrimaryIndexScanContext ctx -> calculatePrimaryIndexSelectors(ctx);
            default ->
                    throw new IllegalArgumentException("Unsupported scan context type: " + context.getClass().getSimpleName());
        };
    }

    private SelectorPair calculatePrimaryIndexSelectors(PrimaryIndexScanContext context) {
        DirectorySubspace indexSubspace = context.indexSubspace();
        ExecutionState state = context.state();
        boolean isReverse = context.isReverse();

        // Base prefix for all entries in this index: [ENTRIES_MAGIC]
        byte[] basePrefix = indexUtils.createIndexEntriesPrefix(indexSubspace);
        KeySelector beginSelector;
        KeySelector endSelector;

        if (isReverse) {
            // Reverse scan: FoundationDB reverses the iteration direction internally
            // We still specify selectors in logical order (begin < end)
            beginSelector = KeySelector.firstGreaterOrEqual(basePrefix);

            if (state.getLower() == null && state.getUpper() == null) {
                // No cursor - scan entire bucket from end to beginning
                endSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesBoundary(indexSubspace));
            } else {
                // Continue reverse scan from cursor position
                // Use upper bound as the stopping point for reverse iteration
                if (state.getUpper() != null) {
                    endSelector = cursorManager.createSelectorFromBound(indexSubspace, state.getUpper());
                } else {
                    endSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesBoundary(indexSubspace));
                }
            }
        } else {
            // Forward scan logic
            if (state.getLower() == null && state.getUpper() == null) {
                // No cursor - start from beginning of bucket
                beginSelector = KeySelector.firstGreaterOrEqual(basePrefix);
            } else {
                // Continue forward scan from last processed position
                beginSelector = cursorManager.createSelectorFromBound(indexSubspace, state.getLower());
            }

            // Always scan to end of bucket for forward scans
            endSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesBoundary(indexSubspace));
        }

        return new SelectorPair(beginSelector, endSelector);
    }

    private SelectorPair calculateIndexScanSelectors(SecondaryIndexScanContext context) {
        DirectorySubspace indexSubspace = context.indexSubspace();
        IndexScanPredicate predicate = context.predicate();
        ExecutionState state = context.state();
        boolean isReverse = context.isReverse();

        // Base prefix for all index entries: [ENTRIES_MAGIC]
        byte[] basePrefix = indexUtils.createIndexEntriesPrefix(indexSubspace);
        KeySelector beginSelector;
        KeySelector endSelector;

        if (isReverse) {
            // Reverse scan: FoundationDB handles direction internally via reverse=true parameter
            // We construct selectors in logical order (begin < end), same as forward scans
            if (state.getLower() == null && state.getUpper() == null) {
                // Fresh scan - construct range from filter conditions only
                KeySelector[] selectors = indexUtils.constructIndexRangeSelectors(indexSubspace, predicate, context.indexDefinition());
                beginSelector = selectors[0]; // Lower bound of filter condition
                endSelector = selectors[1];   // Upper bound of filter condition
            } else {
                // Cursor continuation - combine filter bounds with cursor position
                Bound effectiveLowerBound = getEffectiveLowerBound(predicate, state);

                // Determine start position for continuation
                if (effectiveLowerBound != null) {
                    beginSelector = createSelectorFromLowerBound(indexSubspace, effectiveLowerBound);
                } else {
                    // No effective lower bound - start from index beginning
                    beginSelector = KeySelector.firstGreaterOrEqual(basePrefix);
                }

                // Determine end position - use cursor upper bound or original filter upper bound
                if (state.getUpper() != null) {
                    endSelector = indexUtils.createIndexSelectorFromBound(indexSubspace, state.getUpper());
                } else {
                    endSelector = getUpperBoundSelector(indexSubspace, predicate, context.indexDefinition());
                }
            }
        } else {
            // Forward scan logic
            if (state.getLower() == null && state.getUpper() == null) {
                // Fresh scan - construct range from filter conditions only
                KeySelector[] selectors = indexUtils.constructIndexRangeSelectors(indexSubspace, predicate, context.indexDefinition());
                beginSelector = selectors[0]; // Lower-bound selector from filter
                endSelector = selectors[1];   // Upper-bound selector from filter
            } else {
                // Cursor continuation - determine most restrictive lower bound
                Bound effectiveLowerBound = getEffectiveLowerBound(predicate, state);

                // Check for precise cursor position (includes versionstamp)
                CursorManager.CursorPosition position = cursorManager.getLastProcessedPosition(state, context.nodeId());

                if (position != null) {
                    // Precise positioning: construct exact continuation point
                    Object indexValue = cursorManager.extractIndexValueFromBqlValue(position.indexValue());
                    Tuple cursorTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue, position.versionstamp());
                    byte[] cursorKey_bytes = indexSubspace.pack(cursorTuple);
                    beginSelector = KeySelector.firstGreaterThan(cursorKey_bytes); // Resume after last processed entry
                } else if (effectiveLowerBound != null) {
                    // Use combined bound from cursor and original filter
                    beginSelector = createSelectorFromLowerBound(indexSubspace, effectiveLowerBound);
                } else {
                    // Fallback to original filter's lower bound
                    beginSelector = getLowerBoundSelector(indexSubspace, predicate, context.indexDefinition());
                }

                // Upper bound always comes from original filter (not affected by cursor)
                endSelector = getUpperBoundSelector(indexSubspace, predicate, context.indexDefinition());
            }
        }

        return new SelectorPair(beginSelector, endSelector);
    }

    private KeySelector getLowerBoundSelector(DirectorySubspace indexSubspace, IndexScanPredicate predicate, IndexDefinition indexDefinition) {
        KeySelector[] selectors = indexUtils.constructIndexRangeSelectors(indexSubspace, predicate, indexDefinition);
        return selectors[0];
    }

    private KeySelector getUpperBoundSelector(DirectorySubspace indexSubspace, IndexScanPredicate predicate, IndexDefinition indexDefinition) {
        KeySelector[] selectors = indexUtils.constructIndexRangeSelectors(indexSubspace, predicate, indexDefinition);
        return selectors[1];
    }

    private Bound getEffectiveLowerBound(IndexScanPredicate predicate, ExecutionState state) {
        Bound cursorLower = state.getLower();
        Bound originalLower = getOriginalLowerBound(predicate);

        if (cursorLower == null) return originalLower;
        if (originalLower == null) {
            return cursorLower;
        }

        // Compare bounds and return the more restrictive one (maximum for lower bounds)
        BqlValue cursorValue = cursorLower.value();
        BqlValue originalValue = originalLower.value();

        // For lower bounds, we want the maximum (most restrictive) value
        // This is a simplified comparison - in practice, you'd need proper BqlValue comparison
        return cursorValue.toString().compareTo(originalValue.toString()) > 0 ? cursorLower : originalLower;
    }

    private Bound getOriginalLowerBound(IndexScanPredicate predicate) {
        // Extract lower bound from filter based on operator
        return switch (predicate.op()) {
            case GT -> new Bound(Operator.GT, (BqlValue) predicate.operand());
            case GTE -> new Bound(Operator.GTE, (BqlValue) predicate.operand());
            case LT, LTE, EQ, NE -> null; // These operators don't impose lower bounds
            default -> null;
        };
    }

    private KeySelector createSelectorFromLowerBound(DirectorySubspace indexSubspace, Bound effectiveLowerBound) {
        Object indexValue = indexUtils.extractIndexValue(effectiveLowerBound.value());
        Tuple indexTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue);
        byte[] indexKey = indexSubspace.pack(indexTuple);

        return switch (effectiveLowerBound.operator()) {
            case GT -> {
                // Greater than: start after all entries with this indexed value
                // Use strinc to move past all entries with the exact value, ensuring exclusion
                // This matches the logic in IndexUtils.constructIndexRangeSelectors
                byte[] endKey = ByteArrayUtil.strinc(indexKey);
                yield KeySelector.firstGreaterOrEqual(endKey);
            }
            case GTE -> // Greater than or equal: include entries with exact boundary value
                    KeySelector.firstGreaterOrEqual(indexKey);
            default ->
                    throw new IllegalArgumentException("Unsupported lower bound operator: " + effectiveLowerBound.operator());
        };
    }
}