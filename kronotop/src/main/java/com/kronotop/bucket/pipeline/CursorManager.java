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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.bql.ast.VersionstampVal;
import com.kronotop.bucket.planner.Operator;

import static com.kronotop.bucket.pipeline.IndexUtils.getKeySelector;

/**
 * Manages cursor positioning and checkpoints for paginated query execution in the pipeline.
 *
 * <p>The CursorManager is responsible for tracking the position of query execution across
 * different scan operations, enabling efficient pagination and resumption of queries from
 * specific positions. It handles both forward and reverse scans across primary and secondary
 * indexes.</p>
 *
 * <p><strong>Key Concepts:</strong></p>
 * <ul>
 *   <li><strong>Checkpoint</strong> - Saving the current position in a scan operation</li>
 *   <li><strong>Cursor Position</strong> - A specific location defined by index value and versionstamp</li>
 *   <li><strong>Bounds</strong> - Lower and upper limits for scan operations with inclusive/exclusive flags</li>
 *   <li><strong>Direction Awareness</strong> - Different handling for forward vs. reverse scans</li>
 * </ul>
 *
 * <p><strong>Index Structure Handling:</strong></p>
 * <ul>
 *   <li><strong>Primary Index (_id)</strong> - Uses Versionstamp directly as the key structure</li>
 *   <li><strong>Secondary Indexes</strong> - Uses [BqlValue, Versionstamp] structure</li>
 * </ul>
 *
 * <p><strong>Scan Direction Logic:</strong></p>
 * <ul>
 *   <li><strong>Forward Scans</strong> - Set lower bound (GT) to continue after last processed position</li>
 *   <li><strong>Reverse Scans</strong> - Set upper bound (LT) to continue before last processed position</li>
 * </ul>
 *
 * @since 0.13
 */
public class CursorManager {

    /**
     * Saves a checkpoint for secondary index scan operations.
     *
     * <p>This method stores the current position in a secondary index scan, enabling
     * the query to resume from this exact position in subsequent operations. Secondary
     * indexes use a compound key structure of [BqlValue, Versionstamp] for positioning.</p>
     *
     * <p><strong>Direction-Specific Logic:</strong></p>
     * <ul>
     *   <li><strong>Forward Scans</strong> - Sets lower bound with GT operator to continue after this position</li>
     *   <li><strong>Reverse Scans</strong> - Sets upper bound with LT operator to continue before this position</li>
     * </ul>
     *
     * <p><strong>Key Structure:</strong><br>
     * Secondary Index Key Structure: {@code BqlValue | Versionstamp}<br>
     * The BqlValue represents the indexed field value, and Versionstamp provides unique ordering.</p>
     *
     * @param ctx              the query context containing execution state and options
     * @param nodeId           the identifier of the pipeline node being checkpointed
     * @param lastIndexValue   the last processed BQL value from the secondary index
     * @param lastVersionstamp the versionstamp of the last processed entry for precise positioning
     */
    void saveIndexScanCheckpoint(
            QueryContext ctx,
            int nodeId,
            BqlValue lastIndexValue,
            Versionstamp lastVersionstamp
    ) {
        // Set cursor bounds based on a scan direction
        Operator cursorOperator = ctx.options().isReverse() ? Operator.LT : Operator.GT;

        Bound cursorBound;
        // Secondary Index Key Structure -> BqlValue | Versionstamp
        // For secondary indexes, store BqlValue and set versionstamp on Bound
        cursorBound = new Bound(cursorOperator, lastIndexValue);
        cursorBound.setVersionstamp(lastVersionstamp);

        ExecutionState state = ctx.getOrCreateExecutionState(nodeId);
        if (ctx.options().isReverse()) {
            // For reverse scans: set upper bound (LT) to continue before the last processed
            state.setUpper(cursorBound);
        } else {
            // For forward scans: set lower bound (GT) to continue after last processed
            state.setLower(cursorBound);
        }
    }

    /**
     * Saves a checkpoint for full scan operations on the primary index.
     *
     * <p>This method stores the current position in a full scan operation on the primary
     * index (_id). Full scans traverse all entries in an index without filtering predicates.
     * The primary index uses Versionstamp directly as the key structure.</p>
     *
     * <p><strong>Direction-Specific Logic:</strong></p>
     * <ul>
     *   <li><strong>Forward Scans</strong> - Sets lower bound with GT operator to continue after this versionstamp</li>
     *   <li><strong>Reverse Scans</strong> - Sets upper bound with LT operator to continue before this versionstamp</li>
     * </ul>
     *
     * <p><strong>Key Structure:</strong><br>
     * Primary Index Key Structure: {@code Versionstamp}<br>
     * Uses VersionstampVal wrapper and keeps versionstamp null in Bound since the value itself is the versionstamp.</p>
     *
     * @param ctx              the query context containing execution state and options
     * @param nodeId           the identifier of the pipeline node being checkpointed
     * @param lastVersionstamp the versionstamp of the last processed entry for precise positioning
     */
    void saveFullScanCheckpoint(QueryContext ctx, int nodeId, Versionstamp lastVersionstamp) {
        // Set cursor bounds based on a scan direction
        Operator cursorOperator = ctx.options().isReverse() ? Operator.LT : Operator.GT;

        Bound cursorBound;
        // Primary Index (DefaultIndexDefinition.ID) structure -> Versionstamp
        // For primary index, use VersionstampVal and keep versionstamp null in Bound
        cursorBound = new Bound(cursorOperator, new VersionstampVal(lastVersionstamp));

        ExecutionState state = ctx.getOrCreateExecutionState(nodeId);
        if (ctx.options().isReverse()) {
            // For reverse scans: set upper bound (LT) to continue before the last processed
            state.setUpper(cursorBound);
        } else {
            // For forward scans: set lower bound (GT) to continue after last processed
            state.setLower(cursorBound);
        }
    }

    /**
     * Retrieves the last processed cursor position for continuation of scan operations.
     *
     * <p>This method extracts the cursor position from the execution state based on the scan
     * direction. It looks for the appropriate bound (lower for forward scans, upper for reverse
     * scans) and returns the position information needed to resume scanning.</p>
     *
     * <p><strong>Direction Logic:</strong></p>
     * <ul>
     *   <li><strong>Forward Scans</strong> - Retrieves lower bound (GT) set by previous checkpoint</li>
     *   <li><strong>Reverse Scans</strong> - Retrieves upper bound (LT) set by previous checkpoint</li>
     * </ul>
     *
     * <p><strong>Position Validation:</strong><br>
     * The method requires both a valid bound and a non-null versionstamp for precise positioning.
     * If these conditions are not met, it indicates cursor corruption.</p>
     *
     * @param state     the execution state containing cursor bounds
     * @param nodeId    the identifier of the pipeline node
     * @param isReverse true if this is a reverse scan operation
     * @return the last processed cursor position containing node ID, index value, and versionstamp
     * @throws IllegalStateException if the cursor has been corrupted (missing bounds or versionstamp)
     */
    CursorPosition getLastProcessedPosition(ExecutionState state, int nodeId, boolean isReverse) {
        // Check both lower and upper bounds for cursor position info
        Bound cursorBound = isReverse ? state.getUpper() : state.getLower();
        if (cursorBound != null && cursorBound.versionstamp() != null) {
            return new CursorPosition(nodeId, cursorBound.value(), cursorBound.versionstamp());
        }
        throw new IllegalStateException("Cursor has been corrupted");
    }

    /**
     * Creates a KeySelector from a cursor bound for primary index pagination.
     *
     * <p>This method converts a stored cursor bound into a FoundationDB KeySelector that
     * can be used to resume scanning from the exact position. It's specifically designed
     * for the primary index (_id) which uses Versionstamp values directly.</p>
     *
     * <p><strong>Value Extraction:</strong><br>
     * The method extracts the actual Versionstamp from the VersionstampVal wrapper stored
     * in the bound, then delegates to IndexUtils for KeySelector construction.</p>
     *
     * <p><strong>Error Handling:</strong><br>
     * Validates that the bound is not null and contains a VersionstampVal, which is required
     * for primary index operations.</p>
     *
     * @param idIndexSubspace the primary index (_id) subspace in FoundationDB
     * @param bound           the cursor bound containing position and operator information
     * @return a KeySelector configured for resuming from the bound position
     * @throws IllegalArgumentException if bound is null
     * @throws IllegalStateException    if bound value is not a VersionstampVal (required for _id index)
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
     * Represents a precise cursor position in a scan operation.
     *
     * <p>This record encapsulates the complete information needed to resume a scan operation
     * from an exact position. It contains the pipeline node identifier, the index value that
     * was last processed, and the versionstamp for unique positioning.</p>
     *
     * <p><strong>Usage:</strong><br>
     * CursorPosition instances are created when retrieving checkpoint information and are
     * used by SelectorCalculator to construct appropriate FoundationDB KeySelectors for
     * scan continuation.</p>
     *
     * <p><strong>Components:</strong></p>
     * <ul>
     *   <li><strong>nodeId</strong> - Identifies which pipeline node this position belongs to</li>
     *   <li><strong>indexValue</strong> - The BQL value from the index (can be VersionstampVal for _id index)</li>
     *   <li><strong>versionstamp</strong> - The unique FoundationDB versionstamp for precise positioning</li>
     * </ul>
     *
     * @param nodeId       the identifier of the pipeline node
     * @param indexValue   the BQL value that was last processed from the index
     * @param versionstamp the versionstamp providing unique ordering and positioning
     * @since 0.13
     */
    record CursorPosition(int nodeId, BqlValue indexValue, Versionstamp versionstamp) {
    }
}