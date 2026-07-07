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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.bql.ast.ObjectIdVal;
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.planner.Operator;
import org.bson.types.ObjectId;

import static com.kronotop.bucket.pipeline.IndexUtil.getKeySelector;

/**
 * Manages cursor positioning and checkpoints for paginated query execution in the pipeline.
 *
 * <p>Tracks the scan position across primary (_id → ObjectId) and secondary (BqlValue | ObjectId)
 * indexes, enabling efficient pagination by saving and restoring bounds between requests.</p>
 *
 * <p>Scan direction is controlled by the caller via the {@code scanReversed} flag passed to
 * checkpoint methods. This flag reflects the <em>effective</em> scan direction, which only
 * equals the query's sort direction when the sort field matches the scanned index. Forward
 * scans set a lower bound (GT); reverse scans set an upper bound (LT).</p>
 *
 * @since 0.13
 */
public class CursorManager {

    /**
     * Saves a checkpoint for single field index scan operations.
     *
     * <p>Stores the current scan position using the compound key {@code [BqlValue, ObjectId]}
     * so the query can resume from this exact position in later pagination requests. The
     * {@code scanReversed} parameter controls the effective scan direction, which may differ
     * from the query's sort direction when the sort field does not match the scanned index.</p>
     *
     * @param ctx            the query context containing execution state and options
     * @param nodeId         the identifier of the pipeline node being checkpointed
     * @param lastIndexValue the last processed BQL value from the single field index
     * @param lastObjectId   the ObjectId of the last processed entry for precise positioning
     * @param scanReversed   the effective scan direction; {@code true} sets an upper bound (LT),
     *                       {@code false} sets a lower bound (GT)
     */
    void saveIndexScanCheckpoint(
            QueryContext ctx,
            int nodeId,
            BqlValue lastIndexValue,
            ObjectId lastObjectId,
            boolean scanReversed
    ) {
        // Set cursor bounds based on the effective scan direction
        Operator cursorOperator = scanReversed ? Operator.LT : Operator.GT;

        Bound cursorBound;
        // Single Field Index Key Structure -> BqlValue | ObjectId
        // For single field indexes, store BqlValue and set ObjectId on Bound
        cursorBound = new Bound(cursorOperator, lastIndexValue);
        cursorBound.setObjectId(lastObjectId);

        ExecutionState state = ctx.getOrCreateExecutionState(nodeId);
        if (scanReversed) {
            // For reverse scans: set upper bound (LT) to continue before the last processed
            state.setUpper(cursorBound);
        } else {
            // For forward scans: set lower bound (GT) to continue after last processed
            state.setLower(cursorBound);
        }
    }

    /**
     * Saves an ObjectId-only checkpoint for scan operations that don't track a single field index value.
     *
     * <p>Used by both full scan (primary _id index) and compound index scan nodes. Stores the
     * current scan position so the query can resume from this exact ObjectId in later pagination
     * requests. The {@code scanReversed} parameter controls the effective scan direction, which
     * may differ from the query's sort direction when the sort field does not match the scanned index.</p>
     *
     * @param ctx          the query context containing execution state and options
     * @param nodeId       the identifier of the pipeline node being checkpointed
     * @param lastObjectId the ObjectId of the last processed entry for precise positioning
     * @param scanReversed the effective scan direction; {@code true} sets an upper bound (LT),
     *                     {@code false} sets a lower bound (GT)
     */
    void saveObjectIdCheckpoint(QueryContext ctx, int nodeId, ObjectId lastObjectId, boolean scanReversed) {
        // Set cursor bounds based on the effective scan direction
        Operator cursorOperator = scanReversed ? Operator.LT : Operator.GT;

        Bound cursorBound;
        // Primary Index structure -> ObjectId
        // For primary index, use ObjectIdVal and keep ObjectId null in Bound
        cursorBound = new Bound(cursorOperator, new ObjectIdVal(lastObjectId));

        ExecutionState state = ctx.getOrCreateExecutionState(nodeId);
        if (scanReversed) {
            // For reverse scans: set upper bound (LT) to continue before the last processed
            state.setUpper(cursorBound);
        } else {
            // For forward scans: set lower bound (GT) to continue after last processed
            state.setLower(cursorBound);
        }
    }

    /**
     * Returns the last processed cursor position so a scan can resume.
     *
     * <p>Forward scans (ASC) read the lower bound (GT); reverse scans (DESC) read the upper
     * bound (LT). A valid position needs both the bound and its ObjectId; otherwise the cursor
     * is treated as corrupted.</p>
     *
     * @param state         the execution state containing cursor bounds
     * @param nodeId        the identifier of the pipeline node
     * @param sortDirection ASC for forward scans, DESC for reverse scans
     * @return the last processed cursor position containing node ID, index value, and ObjectId
     * @throws IllegalStateException if the bound or its ObjectId is missing
     */
    CursorPosition getLastProcessedPosition(ExecutionState state, int nodeId, SortDirection sortDirection) {
        // Check both lower and upper bounds for cursor position info
        Bound cursorBound = sortDirection == SortDirection.DESC ? state.getUpper() : state.getLower();
        if (cursorBound != null && cursorBound.objectId() != null) {
            return new CursorPosition(nodeId, cursorBound.value(), cursorBound.objectId());
        }
        throw new IllegalStateException("Cursor has been corrupted");
    }

    /**
     * Builds a FoundationDB KeySelector from a cursor bound for primary index (_id) pagination.
     *
     * <p>The _id index stores ObjectId values directly. The bound's ObjectId is unwrapped from
     * its ObjectIdVal, packed to a byte array, and passed to IndexUtil for KeySelector construction.</p>
     *
     * @param idIndexSubspace the primary index (_id) subspace in FoundationDB
     * @param bound           the cursor bound containing position and operator information
     * @return a KeySelector for resuming from the bound position
     * @throws IllegalArgumentException if bound is null
     * @throws IllegalStateException    if the bound value is not an ObjectIdVal (required for _id index)
     */
    KeySelector createSelectorFromBound(DirectorySubspace idIndexSubspace, Bound bound) {
        if (bound == null) {
            throw new IllegalArgumentException("Bound cannot be null");
        }

        Object boundValue = bound.value();
        if (boundValue instanceof ObjectIdVal(ObjectId value)) {
            boundValue = value.toByteArray(); // Convert ObjectId to byte array for tuple packing
        } else {
            throw new IllegalStateException("Bound value must be ObjectIdVal, got: " + boundValue.getClass().getSimpleName());
        }

        return getKeySelector(idIndexSubspace, bound, boundValue);
    }

    /**
     * Rewinds the cursor to the position of the given entry after over-fetching.
     * Dispatches to the appropriate checkpoint method based on whether the entry
     * carries a single field index value.
     */
    void rewindCursor(QueryContext ctx, int nodeId, DocumentRef lastKeptEntry, boolean scanReversed) {
        BqlValue indexValue = lastKeptEntry.cursorIndexValue();
        if (indexValue != null) {
            saveIndexScanCheckpoint(ctx, nodeId, indexValue, lastKeptEntry.objectId(), scanReversed);
        } else {
            saveObjectIdCheckpoint(ctx, nodeId, lastKeptEntry.objectId(), scanReversed);
        }
    }

    record CursorPosition(int nodeId, BqlValue indexValue, ObjectId objectId) {
    }
}
