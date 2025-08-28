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
            case SecondaryIndexScanContext ctx -> calculateSecondaryIndexScanSelectors(ctx);
            case PrimaryIndexScanContext ctx -> calculatePrimaryIndexSelectors(ctx);
            case RangeScanContext ctx -> calculateRangeScanSelectors(ctx);
            default ->
                    throw new IllegalArgumentException("Unsupported scan context type: " + context.getClass().getSimpleName());
        };
    }

    /**
     * Calculates selectors for range scan operations.
     *
     * <p>Range scans handle queries with both upper and lower bounds, such as:</p>
     * <ul>
     *   <li>{@code age >= 16 AND age <= 40} (inclusive bounds)</li>
     *   <li>{@code price > 10 AND price < 100} (exclusive bounds)</li>
     *   <li>{@code date >= '2023-01-01' AND date < '2024-01-01'} (mixed bounds)</li>
     * </ul>
     *
     * <p><strong>Boundary Handling:</strong></p>
     * <ul>
     *   <li><strong>includeLower=true (GTE)</strong>: Include documents with exact lower bound value</li>
     *   <li><strong>includeLower=false (GT)</strong>: Exclude documents with exact lower bound value</li>
     *   <li><strong>includeUpper=true (LTE)</strong>: Include documents with exact upper bound value</li>
     *   <li><strong>includeUpper=false (LT)</strong>: Exclude documents with exact upper bound value</li>
     * </ul>
     *
     * <p><strong>Key Structure:</strong> {@code [ENTRIES_MAGIC, indexed_value, versionstamp]}</p>
     *
     * <p><strong>Cursor Interaction:</strong> When cursor bounds are present, this method combines
     * them with the original range bounds to ensure proper continuation while respecting the
     * original query constraints.</p>
     *
     * @param context the range scan context containing range bounds, index definition, and cursor state
     * @return selector pair for the range scan boundaries
     */
    private SelectorPair calculateRangeScanSelectors(RangeScanContext context) {
        DirectorySubspace indexSubspace = context.indexSubspace();
        ExecutionState state = context.state();
        RangeScanPredicate predicate = context.predicate();
        boolean isReverse = context.isReverse();

        KeySelector beginSelector;
        KeySelector endSelector;

        if (isReverse) {
            // For reverse scans, use the same selector logic as forward scans
            // The reverse behavior is handled by passing reverse=true to getRange()
            if (state.getLower() == null && state.getUpper() == null) {
                // No cursor - construct range from rangeScan bounds
                KeySelector[] selectors = constructRangeScanSelectors(indexSubspace, predicate);
                beginSelector = selectors[0]; // Lower bound (same as forward)
                endSelector = selectors[1];   // Upper bound (same as forward)
            } else {
                // Check if we have stored cursor state for precise positioning
                CursorManager.CursorPosition position = cursorManager.getLastProcessedPosition(state, context.nodeId());

                if (position != null) {
                    // Construct a precise KeySelector using both index value and versionstamp
                    Object indexValue = cursorManager.extractIndexValueFromBqlValue(position.indexValue());
                    Tuple cursorTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue, position.versionstamp());
                    byte[] cursorKey_bytes = indexSubspace.pack(cursorTuple);
                    endSelector = KeySelector.firstGreaterOrEqual(cursorKey_bytes); // For reverse: end before this position
                } else if (state.getUpper() != null) {
                    endSelector = indexUtils.createIndexSelectorFromBound(indexSubspace, state.getUpper());
                } else {
                    // Use the original range scan upper bound
                    endSelector = getUpperBoundSelectorForRangeScan(indexSubspace, predicate);
                }

                // For reverse scans, begin selector should still be the lower bound
                // The reverse behavior is handled by the reverse=true parameter to getRange()
                beginSelector = getLowerBoundSelectorForRangeScan(indexSubspace, predicate);
            }
        } else {
            // Forward scan logic
            if (state.getLower() == null && state.getUpper() == null) {
                // No cursor - construct range from rangeScan bounds
                KeySelector[] selectors = constructRangeScanSelectors(indexSubspace, predicate);
                beginSelector = selectors[0]; // Lower-bound selector
                endSelector = selectors[1];   // Upper-bound selector
            } else {
                // Combine cursor bounds with original range bounds
                Bound effectiveLowerBound = getEffectiveLowerBoundForRangeScan(predicate, state);

                // Check if we have stored cursor state for precise positioning
                CursorManager.CursorPosition position = cursorManager.getLastProcessedPosition(state, context.nodeId());

                if (position != null) {
                    // Construct a precise KeySelector using both index value and versionstamp
                    Object indexValue = cursorManager.extractIndexValueFromBqlValue(position.indexValue());
                    Tuple cursorTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue, position.versionstamp());
                    byte[] cursorKey_bytes = indexSubspace.pack(cursorTuple);
                    beginSelector = KeySelector.firstGreaterThan(cursorKey_bytes);
                } else if (effectiveLowerBound != null) {
                    Object lowerIndexValue = indexUtils.extractIndexValue(effectiveLowerBound.value());
                    Tuple lowerTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), lowerIndexValue);
                    byte[] lowerKey = indexSubspace.pack(lowerTuple);

                    beginSelector = switch (effectiveLowerBound.operator()) {
                        case GT -> KeySelector.firstGreaterThan(lowerKey);
                        case GTE -> KeySelector.firstGreaterOrEqual(lowerKey);
                        default ->
                                throw new IllegalArgumentException("Unsupported lower bound operator for range scan: " + effectiveLowerBound.operator());
                    };
                } else {
                    beginSelector = getLowerBoundSelectorForRangeScan(indexSubspace, predicate);
                }

                // Use the original range scan upper bound for end selector
                endSelector = getUpperBoundSelectorForRangeScan(indexSubspace, predicate);
            }
        }

        return new SelectorPair(beginSelector, endSelector);
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

    private SelectorPair calculateSecondaryIndexScanSelectors(SecondaryIndexScanContext context) {
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

    private KeySelector getLowerBoundSelectorForRangeScan(DirectorySubspace indexSubspace, RangeScanPredicate predicate) {
        if (predicate.lowerBound() != null) {
            Object lowerIndexValue = indexUtils.extractIndexValue(predicate.lowerBound());
            Tuple lowerTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), lowerIndexValue);
            byte[] lowerKey = indexSubspace.pack(lowerTuple);

            if (predicate.includeLower()) {
                return KeySelector.firstGreaterOrEqual(lowerKey);
            } else {
                return KeySelector.firstGreaterThan(lowerKey);
            }
        } else {
            return KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesPrefix(indexSubspace));
        }
    }

    private KeySelector getUpperBoundSelectorForRangeScan(DirectorySubspace indexSubspace, RangeScanPredicate predicate) {
        if (predicate.upperBound() != null) {
            Object upperIndexValue = indexUtils.extractIndexValue(predicate.upperBound());
            Tuple upperTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), upperIndexValue);
            byte[] upperKey = indexSubspace.pack(upperTuple);

            if (predicate.includeUpper()) {
                // Include upper bound: scan up to and including all entries with this value
                // This matches IndexUtils logic for LTE operations
                byte[] endKey = ByteArrayUtil.strinc(upperKey);
                return KeySelector.firstGreaterOrEqual(endKey);
            } else {
                // Exclude upper bound: scan up to but not including entries with this value
                return KeySelector.firstGreaterOrEqual(upperKey);
            }
        } else {
            return KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesBoundary(indexSubspace));
        }
    }

    private Bound getEffectiveLowerBoundForRangeScan(RangeScanPredicate predicate, ExecutionState state) {
        Bound cursorLower = state.getLower();
        Bound originalLower = null;

        // Create Bound object from rangeScan's lower bound
        if (predicate.lowerBound() != null) {
            Operator lowerOp = predicate.includeLower() ? Operator.GTE : Operator.GT;
            originalLower = new Bound(lowerOp, (BqlValue) predicate.lowerBound());
        }

        if (cursorLower == null) return originalLower;
        if (originalLower == null) {
            return cursorLower;
        }

        // Compare bounds and return the more restrictive one
        BqlValue cursorValue = cursorLower.value();
        BqlValue originalValue = originalLower.value();

        return cursorValue.toString().compareTo(originalValue.toString()) > 0 ? cursorLower : originalLower;
    }

    /**
     * Constructs KeySelector array for range scan operations.
     *
     * <p>This method creates begin and end selectors based on the range scan's upper and lower bounds.
     * It handles both inclusive and exclusive boundaries correctly using FoundationDB's KeySelector API.</p>
     *
     * <p><strong>Boundary Logic:</strong></p>
     * <ul>
     *   <li><strong>Lower Bound (includeLower=true)</strong>: Use firstGreaterOrEqual to include the boundary</li>
     *   <li><strong>Lower Bound (includeLower=false)</strong>: Use firstGreaterThan to exclude the boundary</li>
     *   <li><strong>Upper Bound (includeUpper=true)</strong>: Use strinc + firstGreaterOrEqual to include all entries with the boundary value</li>
     *   <li><strong>Upper Bound (includeUpper=false)</strong>: Use firstGreaterOrEqual to exclude the boundary</li>
     * </ul>
     *
     * @param indexSubspace the index subspace to construct selectors for
     * @return array with [beginSelector, endSelector] for the range
     */
    private KeySelector[] constructRangeScanSelectors(DirectorySubspace indexSubspace, RangeScanPredicate predicate) {
        KeySelector beginSelector;
        KeySelector endSelector;

        // Handle lower bound
        if (predicate.lowerBound() != null) {
            Object lowerIndexValue = indexUtils.extractIndexValue(predicate.lowerBound());
            Tuple lowerTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), lowerIndexValue);
            byte[] lowerKey = indexSubspace.pack(lowerTuple);

            if (predicate.includeLower()) {
                beginSelector = KeySelector.firstGreaterOrEqual(lowerKey);
            } else {
                byte[] endKey = ByteArrayUtil.strinc(lowerKey);
                beginSelector = KeySelector.firstGreaterThan(endKey);
            }
        } else {
            beginSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesPrefix(indexSubspace));
        }

        // Handle upper bound
        if (predicate.upperBound() != null) {
            Object upperIndexValue = indexUtils.extractIndexValue(predicate.upperBound());
            Tuple upperTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), upperIndexValue);
            byte[] upperKey = indexSubspace.pack(upperTuple);

            if (predicate.includeUpper()) {
                // Include upper bound: scan up to and including all entries with this value
                // This matches IndexUtils logic for LTE operations
                byte[] endKey = ByteArrayUtil.strinc(upperKey);
                endSelector = KeySelector.firstGreaterOrEqual(endKey);
            } else {
                // Exclude upper bound: scan up to but not including entries with this value
                endSelector = KeySelector.firstGreaterOrEqual(upperKey);
            }
        } else {
            endSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesBoundary(indexSubspace));
        }

        return new KeySelector[]{beginSelector, endSelector};
    }

}