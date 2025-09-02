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

package com.kronotop.bucket.executor;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.bucket.planner.physical.PhysicalRangeScan;
import com.apple.foundationdb.tuple.ByteArrayUtil;

/**
 * Unified selector calculator for all types of index scans in the execution engine.
 * 
 * <p>This class centralizes the complex logic of determining KeySelector pairs for FoundationDB 
 * range queries across different scan types, cursor states, and scan directions. It ensures 
 * consistent selector calculation behavior throughout the query execution pipeline.</p>
 * 
 * <h2>Key Concepts</h2>
 * <ul>
 *   <li><strong>KeySelector</strong>: FoundationDB's mechanism for specifying range boundaries</li>
 *   <li><strong>Cursor Bounds</strong>: Pagination state tracking the last processed position</li>
 *   <li><strong>Index Subspace</strong>: FoundationDB directory containing index entries</li>
 *   <li><strong>Scan Direction</strong>: Forward (ascending) vs Reverse (descending) iteration</li>
 * </ul>
 * 
 * <h2>Index Key Structure</h2>
 * <ul>
 *   <li><strong>Primary Index</strong>: {@code [ENTRIES_MAGIC, versionstamp]}</li>
 *   <li><strong>Secondary Index</strong>: {@code [ENTRIES_MAGIC, indexed_value, versionstamp]}</li>
 * </ul>
 * 
 * <h2>Supported Scan Types</h2>
 * <ul>
 *   <li><strong>ID Index Scans</strong>: Full bucket scans and OR operations on primary index</li>
 *   <li><strong>Filter Scans</strong>: Single filter conditions (EQ, GT, GTE, LT, LTE, NE)</li>
 *   <li><strong>Range Scans</strong>: Complex range queries with upper/lower bounds</li>
 *   <li><strong>Secondary Index Scans</strong>: Index-based AND operations</li>
 * </ul>
 * 
 * <h2>Cursor Pagination</h2>
 * <p>The calculator handles two types of cursor positioning:</p>
 * <ul>
 *   <li><strong>Precise Cursors</strong>: Include both index value and versionstamp for exact positioning</li>
 *   <li><strong>Bound Cursors</strong>: Use operator bounds (GT, GTE, LT, LTE) for approximate positioning</li>
 * </ul>
 * 
 * <h2>Thread Safety</h2>
 * <p>This class is thread-safe and can be shared across multiple query executions.</p>
 * 
 * @see ScanContext
 * @see SelectorPair
 * @see IndexUtils
 * @see CursorManager
 */
public class SelectorCalculator {
    private final IndexUtils indexUtils;
    private final CursorManager cursorManager;

    /**
     * Creates a new SelectorCalculator with the required dependencies.
     * 
     * @param indexUtils utility class for index operations and key construction
     * @param cursorManager manages cursor state and pagination positioning
     */
    SelectorCalculator(IndexUtils indexUtils, CursorManager cursorManager) {
        this.indexUtils = indexUtils;
        this.cursorManager = cursorManager;
    }

    /**
     * Main entry point for calculating KeySelector pairs based on scan context type.
     * 
     * <p>This method dispatches to the appropriate specialized calculation method based on the
     * provided scan context. Each context type encapsulates the necessary information for
     * computing proper FoundationDB range selectors.</p>
     * 
     * @param context the scan context containing index subspace, bounds, and scan-specific parameters
     * @return a SelectorPair containing begin and end KeySelectors for the FoundationDB range query
     * @throws IllegalArgumentException if the scan context type is not supported
     * 
     * @see IdIndexScanContext
     * @see FilterScanContext
     * @see RangeScanContext
     */
    SelectorPair calculateSelectors(ScanContext context) {
        // Dispatch to specialized calculation method based on context type
        return switch (context) {
            case IdIndexScanContext idCtx -> calculateIdIndexSelectors(idCtx);
            case FilterScanContext filterCtx -> calculateFilterSelectors(filterCtx);
            case RangeScanContext rangeCtx -> calculateRangeSelectors(rangeCtx);
            default ->
                    throw new IllegalArgumentException("Unsupported scan context type: " + context.getClass().getSimpleName());
        };
    }

    /**
     * Calculates selectors for ID index scans (primary index).
     * 
     * <p>ID index scans operate on the primary index which uses versionstamps as keys.
     * These scans are used for:</p>
     * <ul>
     *   <li>Full bucket scans when no filter conditions are present</li>
     *   <li>OR operations that combine results from multiple conditions</li>
     *   <li>Fallback scans when no suitable secondary index is available</li>
     * </ul>
     * 
     * <p><strong>Key Structure:</strong> {@code [ENTRIES_MAGIC, versionstamp]}</p>
     * 
     * @param context the ID index scan context containing subspace and cursor bounds
     * @return selector pair for scanning the primary index range
     */
    private SelectorPair calculateIdIndexSelectors(IdIndexScanContext context) {
        DirectorySubspace indexSubspace = context.indexSubspace();
        Bounds bounds = context.bounds();
        boolean isReverse = context.isReverse();

        // Base prefix for all entries in this index: [ENTRIES_MAGIC]
        byte[] basePrefix = indexUtils.createIndexEntriesPrefix(indexSubspace);
        KeySelector beginSelector;
        KeySelector endSelector;

        if (isReverse) {
            // Reverse scan: FoundationDB reverses the iteration direction internally
            // We still specify selectors in logical order (begin < end)
            beginSelector = KeySelector.firstGreaterOrEqual(basePrefix);
            
            if (bounds == null) {
                // No cursor - scan entire bucket from end to beginning
                endSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesBoundary(indexSubspace));
            } else {
                // Continue reverse scan from cursor position
                // Use upper bound as the stopping point for reverse iteration
                if (bounds.upper() != null) {
                    endSelector = cursorManager.createSelectorFromBound(indexSubspace, bounds.upper());
                } else {
                    endSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesBoundary(indexSubspace));
                }
            }
        } else {
            // Forward scan logic
            if (bounds == null) {
                // No cursor - start from beginning of bucket
                beginSelector = KeySelector.firstGreaterOrEqual(basePrefix);
            } else {
                // Continue forward scan from last processed position
                beginSelector = cursorManager.createSelectorFromBound(indexSubspace, bounds.lower());
            }

            // Always scan to end of bucket for forward scans
            endSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesBoundary(indexSubspace));
        }

        return new SelectorPair(beginSelector, endSelector);
    }

    /**
     * Calculates selectors for filter-based index scans.
     * 
     * <p>Filter scans apply single conditions to secondary indexes using specific operators.
     * This method handles both fresh scans (no cursor) and cursor continuation scans.</p>
     * 
     * <p><strong>Supported Operators:</strong></p>
     * <ul>
     *   <li><strong>EQ</strong>: Equality - scans all entries with exact value match</li>
     *   <li><strong>GT</strong>: Greater than - uses ByteArrayUtil.strinc for proper boundary</li>
     *   <li><strong>GTE</strong>: Greater than or equal - inclusive lower bound</li>
     *   <li><strong>LT</strong>: Less than - exclusive upper bound</li>
     *   <li><strong>LTE</strong>: Less than or equal - inclusive upper bound</li>
     *   <li><strong>NE</strong>: Not equal - requires full scan with manual filtering</li>
     * </ul>
     * 
     * <p><strong>Key Structure:</strong> {@code [ENTRIES_MAGIC, indexed_value, versionstamp]}</p>
     * 
     * <p><strong>Cursor Handling:</strong> When bounds are present, the method combines original
     * filter bounds with cursor bounds to ensure proper continuation from the last processed position.</p>
     * 
     * @param context the filter scan context containing filter, index definition, and cursor bounds
     * @return selector pair for the filtered index scan range
     */
    private SelectorPair calculateFilterSelectors(FilterScanContext context) {
        DirectorySubspace indexSubspace = context.indexSubspace();
        PhysicalFilter filter = context.filter();
        Bounds bounds = context.bounds();
        boolean isReverse = context.isReverse();
        PlanExecutorConfig config = context.config();

        // Base prefix for all index entries: [ENTRIES_MAGIC]
        byte[] basePrefix = indexUtils.createIndexEntriesPrefix(indexSubspace);
        KeySelector beginSelector;
        KeySelector endSelector;

        if (isReverse) {
            // Reverse scan: FoundationDB handles direction internally via reverse=true parameter
            // We construct selectors in logical order (begin < end), same as forward scans
            if (bounds == null) {
                // Fresh scan - construct range from filter conditions only
                KeySelector[] selectors = indexUtils.constructIndexRangeSelectors(indexSubspace, filter, context.indexDefinition());
                beginSelector = selectors[0]; // Lower bound of filter condition
                endSelector = selectors[1];   // Upper bound of filter condition
            } else {
                // Cursor continuation - combine filter bounds with cursor position
                Bound effectiveLowerBound = getEffectiveLowerBound(filter, bounds);

                // Determine start position for continuation
                if (effectiveLowerBound != null) {
                    beginSelector = createSelectorFromLowerBound(indexSubspace, effectiveLowerBound);
                } else {
                    // No effective lower bound - start from index beginning
                    beginSelector = KeySelector.firstGreaterOrEqual(basePrefix);
                }

                // Determine end position - use cursor upper bound or original filter upper bound
                if (bounds.upper() != null) {
                    endSelector = indexUtils.createIndexSelectorFromBound(indexSubspace, bounds.upper());
                } else {
                    endSelector = getUpperBoundSelector(indexSubspace, filter, context.indexDefinition());
                }
            }
        } else {
            // Forward scan logic
            if (bounds == null) {
                // Fresh scan - construct range from filter conditions only
                KeySelector[] selectors = indexUtils.constructIndexRangeSelectors(indexSubspace, filter, context.indexDefinition());
                beginSelector = selectors[0]; // Lower-bound selector from filter
                endSelector = selectors[1];   // Upper-bound selector from filter
            } else {
                // Cursor continuation - determine most restrictive lower bound
                Bound effectiveLowerBound = getEffectiveLowerBound(filter, bounds);

                // Check for precise cursor position (includes versionstamp)
                CursorManager.CursorPosition position = cursorManager.getLastProcessedPosition(config, filter.id());

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
                    beginSelector = getLowerBoundSelector(indexSubspace, filter, context.indexDefinition());
                }

                // Upper bound always comes from original filter (not affected by cursor)
                endSelector = getUpperBoundSelector(indexSubspace, filter, context.indexDefinition());
            }
        }

        return new SelectorPair(beginSelector, endSelector);
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
    private SelectorPair calculateRangeSelectors(RangeScanContext context) {
        DirectorySubspace indexSubspace = context.indexSubspace();
        PhysicalRangeScan rangeScan = context.rangeScan();
        Bounds bounds = context.bounds();
        boolean isReverse = context.isReverse();
        PlanExecutorConfig config = context.config();

        KeySelector beginSelector;
        KeySelector endSelector;

        if (isReverse) {
            // For reverse scans, use the same selector logic as forward scans
            // The reverse behavior is handled by passing reverse=true to getRange()
            if (bounds == null) {
                // No cursor - construct range from rangeScan bounds
                KeySelector[] selectors = constructRangeScanSelectors(indexSubspace, rangeScan);
                beginSelector = selectors[0]; // Lower bound (same as forward)
                endSelector = selectors[1];   // Upper bound (same as forward)
            } else {
                // Check if we have stored cursor state for precise positioning
                CursorManager.CursorPosition position = cursorManager.getLastProcessedPosition(config, rangeScan.id());

                if (position != null) {
                    // Construct a precise KeySelector using both index value and versionstamp
                    Object indexValue = cursorManager.extractIndexValueFromBqlValue(position.indexValue());
                    Tuple cursorTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue, position.versionstamp());
                    byte[] cursorKey_bytes = indexSubspace.pack(cursorTuple);
                    endSelector = KeySelector.firstGreaterOrEqual(cursorKey_bytes); // For reverse: end before this position
                } else if (bounds.upper() != null) {
                    endSelector = indexUtils.createIndexSelectorFromBound(indexSubspace, bounds.upper());
                } else {
                    // Use the original range scan upper bound
                    endSelector = getUpperBoundSelectorForRangeScan(indexSubspace, rangeScan);
                }

                // For reverse scans, begin selector should still be the lower bound
                // The reverse behavior is handled by the reverse=true parameter to getRange()
                beginSelector = getLowerBoundSelectorForRangeScan(indexSubspace, rangeScan);
            }
        } else {
            // Forward scan logic
            if (bounds == null) {
                // No cursor - construct range from rangeScan bounds
                KeySelector[] selectors = constructRangeScanSelectors(indexSubspace, rangeScan);
                beginSelector = selectors[0]; // Lower-bound selector
                endSelector = selectors[1];   // Upper-bound selector
            } else {
                // Combine cursor bounds with original range bounds
                Bound effectiveLowerBound = getEffectiveLowerBoundForRangeScan(rangeScan, bounds);

                // Check if we have stored cursor state for precise positioning
                CursorManager.CursorPosition position = cursorManager.getLastProcessedPosition(config, rangeScan.id());

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
                    beginSelector = getLowerBoundSelectorForRangeScan(indexSubspace, rangeScan);
                }

                // Use the original range scan upper bound for end selector
                endSelector = getUpperBoundSelectorForRangeScan(indexSubspace, rangeScan);
            }
        }

        return new SelectorPair(beginSelector, endSelector);
    }

    /**
     * Creates cursor-aware scan range selectors for general scanning operations.
     * 
     * <p>This method provides a general-purpose selector calculation that handles cursor
     * continuation for operations that don't have specific filter or range constraints.
     * It's primarily used by:</p>
     * <ul>
     *   <li>OR operations that need to scan broad ranges</li>
     *   <li>Mixed operations combining different scan types</li>
     *   <li>Secondary index scans without specific conditions</li>
     * </ul>
     * 
     * <p>The method creates a range from the cursor's lower bound (if present) to the
     * end of the index subspace, ensuring complete coverage while respecting cursor state.</p>
     * 
     * @param indexSubspace the index subspace (typically primary index) to scan
     * @param bounds cursor bounds containing lower bound positioning information, may be null
     * @return selector pair for cursor-aware general scanning
     */
    SelectorPair calculateCursorAwareScanRange(DirectorySubspace indexSubspace, Bounds bounds) {
        KeySelector beginSelector;
        
        // Determine start position based on cursor bounds
        if (bounds != null) {
            Bound cursorLowerBound = bounds.lower();
            if (cursorLowerBound != null) {
                // Continue from last processed position
                beginSelector = cursorManager.createSelectorFromBound(indexSubspace, cursorLowerBound);
            } else {
                // No specific lower bound - start from index beginning
                beginSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesPrefix(indexSubspace));
            }
        } else {
            // No cursor bounds - fresh scan from index beginning
            beginSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesPrefix(indexSubspace));
        }

        // Always scan to end of index subspace for general operations
        KeySelector endSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesBoundary(indexSubspace));

        return new SelectorPair(beginSelector, endSelector);
    }

    // ========================================
    // Helper Methods
    // ========================================
    // 
    // The following methods provide specialized selector calculation logic
    // extracted from the original ExecutionHandlers implementation.


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
     * @param rangeScan the range scan containing upper/lower bounds and inclusion flags
     * @return array with [beginSelector, endSelector] for the range
     */
    private KeySelector[] constructRangeScanSelectors(DirectorySubspace indexSubspace, PhysicalRangeScan rangeScan) {
        KeySelector beginSelector;
        KeySelector endSelector;

        // Handle lower bound
        if (rangeScan.lowerBound() != null) {
            Object lowerIndexValue = indexUtils.extractIndexValue(rangeScan.lowerBound());
            Tuple lowerTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), lowerIndexValue);
            byte[] lowerKey = indexSubspace.pack(lowerTuple);

            if (rangeScan.includeLower()) {
                beginSelector = KeySelector.firstGreaterOrEqual(lowerKey);
            } else {
                beginSelector = KeySelector.firstGreaterThan(lowerKey);
            }
        } else {
            beginSelector = KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesPrefix(indexSubspace));
        }

        // Handle upper bound
        if (rangeScan.upperBound() != null) {
            Object upperIndexValue = indexUtils.extractIndexValue(rangeScan.upperBound());
            Tuple upperTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), upperIndexValue);
            byte[] upperKey = indexSubspace.pack(upperTuple);

            if (rangeScan.includeUpper()) {
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

    private KeySelector getLowerBoundSelector(DirectorySubspace indexSubspace, PhysicalFilter filter, IndexDefinition indexDefinition) {
        KeySelector[] selectors = indexUtils.constructIndexRangeSelectors(indexSubspace, filter, indexDefinition);
        return selectors[0];
    }

    private KeySelector getUpperBoundSelector(DirectorySubspace indexSubspace, PhysicalFilter filter, IndexDefinition indexDefinition) {
        KeySelector[] selectors = indexUtils.constructIndexRangeSelectors(indexSubspace, filter, indexDefinition);
        return selectors[1];
    }

    private KeySelector getLowerBoundSelectorForRangeScan(DirectorySubspace indexSubspace, PhysicalRangeScan rangeScan) {
        if (rangeScan.lowerBound() != null) {
            Object lowerIndexValue = indexUtils.extractIndexValue(rangeScan.lowerBound());
            Tuple lowerTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), lowerIndexValue);
            byte[] lowerKey = indexSubspace.pack(lowerTuple);

            if (rangeScan.includeLower()) {
                return KeySelector.firstGreaterOrEqual(lowerKey);
            } else {
                return KeySelector.firstGreaterThan(lowerKey);
            }
        } else {
            return KeySelector.firstGreaterOrEqual(indexUtils.createIndexEntriesPrefix(indexSubspace));
        }
    }

    private KeySelector getUpperBoundSelectorForRangeScan(DirectorySubspace indexSubspace, PhysicalRangeScan rangeScan) {
        if (rangeScan.upperBound() != null) {
            Object upperIndexValue = indexUtils.extractIndexValue(rangeScan.upperBound());
            Tuple upperTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), upperIndexValue);
            byte[] upperKey = indexSubspace.pack(upperTuple);

            if (rangeScan.includeUpper()) {
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

    private Bound getEffectiveLowerBound(PhysicalFilter filter, Bounds cursorBounds) {
        Bound cursorLower = cursorBounds.lower();
        Bound originalLower = getOriginalLowerBound(filter);

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

    private Bound getEffectiveLowerBoundForRangeScan(PhysicalRangeScan rangeScan, Bounds cursorBounds) {
        Bound cursorLower = cursorBounds.lower();
        Bound originalLower = null;

        // Create Bound object from rangeScan's lower bound
        if (rangeScan.lowerBound() != null) {
            Operator lowerOp = rangeScan.includeLower() ? Operator.GTE : Operator.GT;
            originalLower = new Bound(lowerOp, (BqlValue) rangeScan.lowerBound());
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
     * Extracts the original lower bound from a filter condition.
     * 
     * <p>This method determines if a filter condition imposes a lower bound constraint
     * and creates the appropriate Bound object. Only GT and GTE operators create
     * lower bounds - other operators either create upper bounds or require full scans.</p>
     * 
     * <p><strong>Operator Mapping:</strong></p>
     * <ul>
     *   <li><strong>GT</strong>: Creates exclusive lower bound (value must be greater than operand)</li>
     *   <li><strong>GTE</strong>: Creates inclusive lower bound (value must be greater than or equal to operand)</li>
     *   <li><strong>LT, LTE, EQ, NE</strong>: No lower bound (returns null)</li>
     * </ul>
     * 
     * @param filter the physical filter to analyze
     * @return Bound object representing the lower bound, or null if no lower bound exists
     */
    private Bound getOriginalLowerBound(PhysicalFilter filter) {
        // Extract lower bound from filter based on operator
        return switch (filter.op()) {
            case GT -> new Bound(Operator.GT, (BqlValue) filter.operand());
            case GTE -> new Bound(Operator.GTE, (BqlValue) filter.operand());
            case LT, LTE, EQ, NE -> null; // These operators don't impose lower bounds
            default -> null;
        };
    }

    /**
     * Creates a KeySelector from a lower bound constraint.
     * 
     * <p>This method converts a Bound object (containing operator and value) into the appropriate
     * FoundationDB KeySelector for range scanning. The implementation ensures consistency with
     * the IndexUtils.constructIndexRangeSelectors logic.</p>
     * 
     * <p><strong>Critical Implementation Details:</strong></p>
     * <ul>
     *   <li><strong>GT (Greater Than)</strong>: Uses ByteArrayUtil.strinc to position after all entries 
     *       with the boundary value. This ensures documents with exact boundary value are excluded.</li>
     *   <li><strong>GTE (Greater Than or Equal)</strong>: Uses firstGreaterOrEqual to include documents 
     *       with exact boundary value.</li>
     * </ul>
     * 
     * <p><strong>Bug Fix Note:</strong> The GT implementation was fixed to use strinc + firstGreaterOrEqual
     * instead of firstGreaterThan to match IndexUtils behavior and ensure correct boundary exclusion.</p>
     * 
     * @param indexSubspace the index subspace for key construction
     * @param effectiveLowerBound the bound containing operator and value information
     * @return KeySelector positioned at the appropriate lower boundary
     * @throws IllegalArgumentException if the bound operator is not GT or GTE
     */
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