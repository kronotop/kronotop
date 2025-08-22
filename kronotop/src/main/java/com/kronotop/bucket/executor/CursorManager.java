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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.Operator;
import org.bson.BsonType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.kronotop.bucket.executor.IndexUtils.getKeySelector;

/**
 * The CursorManager class manages cursor positioning and pagination boundaries for index
 * scans and query executions. It provides methods to set cursor boundaries, handle cases
 * with empty results, and translate values between index storage and query operations.
 * <p>
 * It is designed to work with forward and reverse scans, supports pagination, and ensures
 * cursor advancement to avoid infinite loops when processing filter criteria. The class
 * also provides functionalities to create and extract values for specific types used in index operations.
 */
class CursorManager {
    /**
     * Sets cursor boundaries based on results for pagination.
     */
    void setCursorBoundaries(PlanExecutorConfig config, int nodeId, Map<Versionstamp, ByteBuffer> results) {
        Bounds newBounds = calculateCursorBounds(config, results);
        config.cursor().setBounds(nodeId, newBounds);
    }

    /**
     * Sets cursor boundaries when no results match the filter but index entries were processed.
     * This ensures cursor advancement to avoid infinite loops when filters match few documents.
     *
     * @param config           the plan executor configuration containing cursor state
     * @param lastProcessedKey the last document ID that was processed (but didn't match filter)
     */
    void setCursorBoundariesForEmptyResults(PlanExecutorConfig config, int nodeId, Versionstamp lastProcessedKey) {
        Operator cursorOperator;

        if (config.isReverse()) {
            // For reverse scans, the next batch should be before the last processed key
            cursorOperator = Operator.LT;
        } else {
            // For forward scans, the next batch should be after the last processed key
            cursorOperator = Operator.GT;
        }

        // Create the cursor bound for the ID index
        Bound cursorBound = new Bound(cursorOperator, new VersionstampVal(lastProcessedKey));

        // Set the appropriate boundary based on the scan direction
        Bounds newBounds;
        if (config.isReverse()) {
            // For reverse scans: set upper bound (LT) to continue before the last processed key
            newBounds = new Bounds(null, cursorBound);
        } else {
            // For forward scans: set lower bound (GT) to continue after the last processed key
            newBounds = new Bounds(cursorBound, null);
        }

        config.cursor().setBounds(nodeId, newBounds);
    }

    /**
     * Creates a KeySelector from a cursor bound for pagination.
     *
     * @param idIndexSubspace the ID index subspace
     * @param bound           the bound containing the cursor position
     * @return appropriate KeySelector for the bound
     */
    KeySelector createSelectorFromBound(DirectorySubspace idIndexSubspace, Bound bound) {
        if (bound == null) {
            throw new IllegalArgumentException("Bound cannot be null");
        }

        Object boundValue = bound.value();
        if (boundValue instanceof VersionstampVal(Versionstamp value)) {
            boundValue = value; // Extract the actual Versionstamp
        } else {
            throw new IllegalStateException("Bound value must be VersionstampVal, got: " + boundValue.getClass().getSimpleName());
        }

        return getKeySelector(idIndexSubspace, bound, boundValue);
    }

    /**
     * Sets cursor bounds for index scans with specific positioning.
     */
    void setCursorBoundsForIndexScan(
            PlanExecutorConfig config,
            int nodeId,
            IndexDefinition definition,
            BqlValue lastIndexValue,
            Versionstamp lastVersionstamp
    ) {
        // Set cursor bounds based on a scan direction
        Operator cursorOperator = config.isReverse() ? Operator.LT : Operator.GT;

        Bound cursorBound;
        if (DefaultIndexDefinition.ID.selector().equals(definition.selector())) {
            // Primary Index (DefaultIndexDefinition.ID) structure -> Versionstamp
            // For primary index, use VersionstampVal and keep versionstamp null in Bound
            cursorBound = new Bound(cursorOperator, new VersionstampVal(lastVersionstamp));
        } else {
            // Secondary Index Key Structure -> BqlValue | Versionstamp  
            // For secondary indexes, store BqlValue and set versionstamp on Bound
            cursorBound = new Bound(cursorOperator, lastIndexValue);
            cursorBound.setVersionstamp(lastVersionstamp);
        }

        Bounds cursorBounds;
        if (config.isReverse()) {
            // For reverse scans: set upper bound (LT) to continue before the last processed
            cursorBounds = new Bounds(null, cursorBound);
        } else {
            // For forward scans: set lower bound (GT) to continue after last processed
            cursorBounds = new Bounds(cursorBound, null);
        }

        config.cursor().setState(nodeId, new CursorState(cursorBounds));
    }

    /**
     * Creates a BqlValue from a raw index value based on the BSON type.
     */
    BqlValue createBqlValueFromIndexValue(Object value, BsonType bsonType) {
        return switch (bsonType) {
            case STRING -> new StringVal((String) value);
            case INT32 -> new Int32Val(((Long) value).intValue()); // Index stores INT32 as long
            case INT64 -> new Int64Val((Long) value);
            case DOUBLE -> new DoubleVal((Double) value);
            case BOOLEAN -> new BooleanVal((Boolean) value);
            case DATE_TIME -> new DateTimeVal((Long) value);
            case TIMESTAMP -> new TimestampVal((Long) value);
            case DECIMAL128 -> new Decimal128Val(new BigDecimal((String) value));
            case BINARY -> {
                if (value instanceof Versionstamp) {
                    // For _id index, create a VersionstampVal instead of BinaryVal
                    yield new VersionstampVal((Versionstamp) value);
                } else {
                    // For regular binary indexes
                    yield new BinaryVal((byte[]) value);
                }
            }
            case NULL -> new NullVal();
            default -> throw new IllegalArgumentException("Unsupported BSON type for index value: " + bsonType);
        };
    }

    /**
     * Extracts the raw index value from a BqlValue for index operations.
     */
    Object extractIndexValueFromBqlValue(BqlValue bqlValue) {
        return switch (bqlValue) {
            case StringVal stringVal -> stringVal.value();
            case Int32Val int32Val -> (long) int32Val.value(); // Store INT32 as long in index
            case Int64Val int64Val -> int64Val.value();
            case DoubleVal doubleVal -> doubleVal.value();
            case BooleanVal booleanVal -> booleanVal.value();
            case DateTimeVal dateTimeVal -> dateTimeVal.value();
            case TimestampVal timestampVal -> timestampVal.value();
            case Decimal128Val decimal128Val -> decimal128Val.value().toString();
            case BinaryVal binaryVal -> binaryVal.value();
            case VersionstampVal versionstampVal -> versionstampVal.value();
            case NullVal ignored -> null;
            default ->
                    throw new IllegalArgumentException("Unsupported BqlValue type for index: " + bqlValue.getClass().getSimpleName());
        };
    }

    /**
     * Sets cursor for non-indexed query paths based on the last result returned.
     *
     * <p>This method is specifically designed for non-indexed operations that scan the ID index
     * and apply filters at the document level (e.g., executeNonIndexedAnd, executeNonIndexedOr).
     * It advances the cursor to continue from the last processed document in the next iteration.</p>
     *
     * <p><strong>Use Cases:</strong></p>
     * <ul>
     *   <li>Non-indexed AND operations - full ID index scan with document filtering</li>
     *   <li>Non-indexed OR operations - full ID index scan with OR filter logic</li>
     *   <li>Any operation that processes documents sequentially via ID index</li>
     * </ul>
     *
     * @param config  plan executor configuration containing cursor state
     * @param nodeId  the physical node ID to set cursor for
     * @param results the result map containing processed documents
     */
    void setCursorFromLastResultForNonIndexedPath(PlanExecutorConfig config, int nodeId, Map<Versionstamp, ByteBuffer> results) {
        if (results.isEmpty()) {
            return;
        }

        // Get the last (highest) Versionstamp from results - this represents
        // the furthest point we've processed in the ID index
        Versionstamp lastVersionstamp = results.keySet().stream()
                .max(Versionstamp::compareTo)
                .orElse(null);

        // Set cursor to continue from after the last processed document
        // Use GT (greater than) to exclude the already processed document
        Bound cursorBound = new Bound(Operator.GT, new VersionstampVal(lastVersionstamp));
        Bounds newBounds = new Bounds(cursorBound, null);
        config.cursor().setState(nodeId, new CursorState(newBounds));
    }

    /**
     * Gets an aggregated cursor versionstamp from multiple cursor states.
     * For multi-index queries, this returns the most restrictive cursor position.
     *
     * @param config the plan executor configuration containing cursor states
     * @return the most restrictive cursor versionstamp, or null if no cursors exist
     */
    Versionstamp getAggregatedCursorVersionstamp(PlanExecutorConfig config) {
        // For forward scans: return the maximum versionstamp (most restrictive)
        // For reverse scans: return the minimum versionstamp (most restrictive)

        // Check all node-specific cursor states
        List<CursorState> idIndexStates = config.cursor().getAllCursorStates();
        return aggregateVersionstampFromCursorStates(config, idIndexStates);
    }

    /**
     * Checks if any node has cursor state in the configuration.
     * Useful for determining whether cursor-aware scanning is needed.
     *
     * @param config the plan executor configuration to check
     * @return true if any cursor state exists, false otherwise
     */
    boolean hasAnyCursorPosition(PlanExecutorConfig config) {
        return !config.cursor().getAllCursorStates().isEmpty() ||
                config.cursor().size() > 0;
    }

    /**
     * Gets cursor information optimized for specific query contexts.
     * This method selects the best cursor strategy based on the query type.
     *
     * @param config  the plan executor configuration
     * @param context the query execution context
     * @return the optimal cursor versionstamp for the given context
     */
    Versionstamp getContextAwareCursorVersionstamp(PlanExecutorConfig config, CompositeQueryContext context) {
        if (!hasAnyCursorPosition(config)) {
            return null;
        }

        return switch (context.queryType()) {
            case RANGE_SCAN -> {
                // For single index operations, use a node-specific cursor if available
                if (!context.activeNodeIds().isEmpty()) {
                    int nodeId = context.activeNodeIds().iterator().next();
                    Bounds bounds = config.cursor().getBounds(nodeId);
                    if (bounds != null && bounds.lower() != null) {
                        Bound lowerBound = bounds.lower();
                        if (lowerBound.value() instanceof VersionstampVal(Versionstamp value)) {
                            yield value;
                        }
                    }
                }
                yield getAggregatedCursorVersionstamp(config);
            }

            case MULTI_INDEX_AND -> // For AND operations, use the most restrictive cursor (furthest progress)
                    getAggregatedCursorVersionstamp(config);

            case MULTI_INDEX_OR -> // For OR operations, use the least restrictive cursor (earliest position)
                    getLeastRestrictiveCursorVersionstamp(config);

            case MIXED_QUERY, FULL_BUCKET_SCAN -> // For mixed queries and full scans, use aggregated approach
                    getAggregatedCursorVersionstamp(config);
        };
    }

    /**
     * Gets the least restrictive cursor position for OR operations.
     * In OR queries, we want to continue from the earliest position to avoid missing results.
     *
     * @param config the plan executor configuration
     * @return the least restrictive cursor versionstamp
     */
    Versionstamp getLeastRestrictiveCursorVersionstamp(PlanExecutorConfig config) {
        Versionstamp result = null;

        List<CursorState> idIndexStates = config.cursor().getAllCursorStates();
        for (CursorState state : idIndexStates) {
            if (state.bounds() != null && state.bounds().lower() != null) {
                Bound lowerBound = state.bounds().lower();
                if (lowerBound.value() instanceof VersionstampVal(Versionstamp value)) {
                    if (result == null) {
                        result = value;
                    } else {
                        // For OR operations, take the opposite of the restrictive logic
                        if (config.isReverse()) {
                            // For reverse scans, larger versionstamps are less restrictive
                            result = value.compareTo(result) > 0 ? value : result;
                        } else {
                            // For forward scans, smaller versionstamps are less restrictive
                            result = value.compareTo(result) < 0 ? value : result;
                        }
                    }
                }
            }
        }

        return result;
    }


    /**
     * Retrieves the last processed cursor position for the specified index definition,
     * based on the bounds stored in the provided plan executor configuration.
     * If no bounds or versionstamp information is available, this method returns null.
     *
     * @param config the plan executor configuration containing cursor state and bounds
     * @return the last processed cursor position as a {@code CursorPosition} object, or null if unavailable
     */
    CursorPosition getLastProcessedPosition(PlanExecutorConfig config, int nodeId) {
        // TODO: REFACTOR
        Bounds bounds = config.cursor().getBounds(nodeId);
        if (bounds == null) {
            return null;
        }

        // Check both lower and upper bounds for cursor position info
        Bound cursorBound = bounds.lower() != null ? bounds.lower() : bounds.upper();
        if (cursorBound != null && cursorBound.versionstamp() != null) {
            return new CursorPosition(nodeId, cursorBound.value(), cursorBound.versionstamp());
        }

        return null;
    }

    /**
     * Sets cursor bounds based on scanner positions for multi-index operations.
     * This ensures all involved indexes advance past their last processed positions.
     *
     * @param config           the plan executor configuration containing cursor state
     * @param scannerPositions map of index definitions to their last processed positions
     * @param results          the final results to also consider for cursor advancement
     */
    void setCursorFromScannerPositions(PlanExecutorConfig config,
                                       Map<IndexDefinition, CursorPosition> scannerPositions,
                                       Map<Versionstamp, ByteBuffer> results) {
        if (scannerPositions.isEmpty()) {
            return;
        }

        // Set cursor bounds for each scanned index
        for (Map.Entry<IndexDefinition, CursorPosition> entry : scannerPositions.entrySet()) {
            IndexDefinition indexDef = entry.getKey();
            CursorPosition position = entry.getValue();

            if (position.versionstamp() != null) {
                setCursorBoundsForIndexScan(config, position.nodeId(), indexDef, position.indexValue(), position.versionstamp());
            }
        }

        // Also ensure primary index cursor is advanced if we have results
        if (!results.isEmpty()) {
            Versionstamp lastVersionstamp;
            Operator cursorOperator;

            if (config.isReverse()) {
                // For reverse scans, use the minimum versionstamp and LT operator
                lastVersionstamp = results.keySet().stream()
                        .min(Versionstamp::compareTo)
                        .orElse(null);
                cursorOperator = Operator.LT;
            } else {
                // For forward scans, use the maximum versionstamp and GT operator
                lastVersionstamp = results.keySet().stream()
                        .max(Versionstamp::compareTo)
                        .orElse(null);
                cursorOperator = Operator.GT;
            }

            Bound cursorBound = new Bound(cursorOperator, new VersionstampVal(lastVersionstamp));
            Bounds newBounds = new Bounds(cursorBound, null);
            // Use node-specific cursor state instead of deprecated bounds map
            // Note: Using nodeId from scannerPositions if available, otherwise fall back to a default node
            int nodeId = scannerPositions.values().stream()
                    .findFirst()
                    .map(CursorPosition::nodeId)
                    .orElse(0); // Default node ID
            config.cursor().setState(nodeId, new CursorState(newBounds));
        }
    }



    /**
     * Sets cursor from mixed query results using node ID tracking.
     * This sets cursors for both indexed and non-indexed portions based on their node IDs.
     *
     * @param config               the plan executor configuration
     * @param indexedNodeId        the indexed portion node ID
     * @param nonIndexedNodeId     the non-indexed portion node ID
     * @param indexResults         results from indexed portion
     * @param nonIndexResults      results from non-indexed portion
     */
    void setMixedQueryCursor(PlanExecutorConfig config,
                            int indexedNodeId,
                            int nonIndexedNodeId,
                            Map<Versionstamp, ByteBuffer> indexResults,
                            Map<Versionstamp, ByteBuffer> nonIndexResults) {

        // Set cursor for indexed node if it has results
        if (!indexResults.isEmpty()) {
            setCursorBoundaries(config, indexedNodeId, indexResults);
        }

        // Set cursor for non-indexed node if it has results
        if (!nonIndexResults.isEmpty()) {
            setCursorBoundaries(config, nonIndexedNodeId, nonIndexResults);
        }
    }

    // ========================================
    // Node-Based Cursor Management
    // ========================================

    /**
     * Advanced cursor management for mixed queries with multiple iterations.
     * This method handles the complex coordination needed for proper pagination
     * across different node types.
     *
     * @param config                  the plan executor configuration
     * @param currentIterationResults results from the current iteration
     * @param nodeStates             map of node states for coordination
     */
    @SuppressWarnings("unchecked")
    public void advanceMixedQueryCursors(PlanExecutorConfig config,
                                         Map<Versionstamp, ?> currentIterationResults,
                                         Map<Integer, NodeExecutionState> nodeStates) {

        if (currentIterationResults.isEmpty() || nodeStates.isEmpty()) {
            return;
        }

        // Determine the overall cursor advancement strategy
        Versionstamp overallCursor = config.isReverse()
                ? currentIterationResults.keySet().stream().min(Versionstamp::compareTo).orElse(null)
                : currentIterationResults.keySet().stream().max(Versionstamp::compareTo).orElse(null);

        // Update each node's cursor based on its participation in this iteration
        for (Map.Entry<Integer, NodeExecutionState> entry : nodeStates.entrySet()) {
            int nodeId = entry.getKey();
            NodeExecutionState state = entry.getValue();
            
            if (state != null && state.hasResults()) {
                // This node contributed results, advance its cursor
                setCursorBoundaries(config, nodeId, (Map<Versionstamp, ByteBuffer>) state.results());
            }
        }

        // Set the overall coordinated cursor for the mixed query
        if (overallCursor != null) {
            Operator cursorOperator = config.isReverse() ? Operator.LT : Operator.GT;
            Bound cursorBound = new Bound(cursorOperator, new VersionstampVal(overallCursor));
            Bounds newBounds = new Bounds(
                    config.isReverse() ? null : cursorBound,
                    config.isReverse() ? cursorBound : null
            );

            int coordinatorNodeId = 0; // Default node ID for mixed query coordination
            config.cursor().setState(coordinatorNodeId, new CursorState(newBounds));
        }
    }

    // ========================================
    // Enhanced Cursor Coordination for Mixed Queries
    // ========================================



    /**
     * Aggregates versionstamps from cursor states to find the most restrictive position.
     * Extracted helper method to eliminate code duplication between regular and node-based cursor management.
     *
     * @param config the plan executor configuration containing scan direction
     * @param states the cursor states to aggregate versionstamps from
     * @return the most restrictive versionstamp, or null if no valid versionstamps found
     */
    private Versionstamp aggregateVersionstampFromCursorStates(PlanExecutorConfig config, List<CursorState> states) {
        Versionstamp result = null;

        for (CursorState state : states) {
            if (state.bounds() != null && state.bounds().lower() != null) {
                Bound lowerBound = state.bounds().lower();
                if (lowerBound.value() instanceof VersionstampVal(Versionstamp value)) {
                    if (result == null) {
                        result = value;
                    } else {
                        // For forward scans, take the maximum (most restrictive)
                        // For reverse scans, take the minimum (most restrictive)
                        if (config.isReverse()) {
                            result = value.compareTo(result) < 0 ? value : result;
                        } else {
                            result = value.compareTo(result) > 0 ? value : result;
                        }
                    }
                }
            }
        }

        return result;
    }

    /**
     * Calculates cursor bounds from query results based on scan direction.
     * Extracted helper method to eliminate code duplication between regular and node-based cursor management.
     *
     * @param config  the plan executor configuration containing scan direction
     * @param results the results map to calculate cursor bounds from
     * @return the calculated cursor bounds for pagination
     */
    private Bounds calculateCursorBounds(PlanExecutorConfig config, Map<Versionstamp, ByteBuffer> results) {
        Versionstamp cursorKey;
        Operator cursorOperator;

        if (config.isReverse()) {
            // For reverse scans, the cursor should continue from the smallest document in results
            // Since we're going backwards, the next batch should be before this key
            cursorKey = results.keySet().stream().min(Versionstamp::compareTo).orElseThrow();
            cursorOperator = Operator.LT; // The next batch should be less than this key
        } else {
            // For forward scans, the cursor should continue from the largest document in results
            cursorKey = results.keySet().stream().max(Versionstamp::compareTo).orElseThrow();
            cursorOperator = Operator.GT; // The next batch should be greater than this key
        }

        // Create the cursor bound for the ID index (used for scanning)
        Bound cursorBound = new Bound(cursorOperator, new VersionstampVal(cursorKey));

        // For cursor pagination, we only set the appropriate boundary based on a scan direction
        if (config.isReverse()) {
            // For reverse scans: set upper bound (LT) to continue before current results
            return new Bounds(null, cursorBound);
        } else {
            // For forward scans: set lower bound (GT) to continue after current results
            return new Bounds(cursorBound, null);
        }
    }

    /**
     * Gets the last processed index value and versionstamp for a definition from stored cursor bounds.
     * Returns null if no cursor bounds exist or they don't contain precise positioning info.
     */
    record CursorPosition(int nodeId, BqlValue indexValue, Versionstamp versionstamp) {
    }

    /**
     * Represents the execution state of a node during mixed query processing.
     */
    public record NodeExecutionState(Map<Versionstamp, ?> results, boolean exhausted) {

        public boolean hasResults() {
            return results != null && !results.isEmpty();
        }
    }
}