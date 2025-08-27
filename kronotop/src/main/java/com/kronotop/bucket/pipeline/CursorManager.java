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
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.Operator;

import static com.kronotop.bucket.pipeline.IndexUtils.getKeySelector;

public class CursorManager {

    /**
     * Sets cursor bounds for secondary index scans with specific positioning.
     */
    void saveSecondaryIndexCheckpoint(
            PipelineContext ctx,
            int nodeId,
            BqlValue lastIndexValue,
            Versionstamp lastVersionstamp
    ) {
        // Set cursor bounds based on a scan direction
        Operator cursorOperator = ctx.isReverse() ? Operator.LT : Operator.GT;

        Bound cursorBound;
        // Secondary Index Key Structure -> BqlValue | Versionstamp
        // For secondary indexes, store BqlValue and set versionstamp on Bound
        cursorBound = new Bound(cursorOperator, lastIndexValue);
        cursorBound.setVersionstamp(lastVersionstamp);

        ExecutionState state = ctx.getOrCreateExecutionState(nodeId);
        if (ctx.isReverse()) {
            // For reverse scans: set upper bound (LT) to continue before the last processed
            state.setUpper(cursorBound);
            state.setLower(null);
        } else {
            // For forward scans: set lower bound (GT) to continue after last processed
            state.setLower(cursorBound);
            state.setUpper(null);
        }
    }

    void savePrimaryIndexCheckpoint(PipelineContext ctx, int nodeId, Versionstamp lastVersionstamp) {
        // Set cursor bounds based on a scan direction
        Operator cursorOperator = ctx.isReverse() ? Operator.LT : Operator.GT;

        Bound cursorBound;
        // Primary Index (DefaultIndexDefinition.ID) structure -> Versionstamp
        // For primary index, use VersionstampVal and keep versionstamp null in Bound
        cursorBound = new Bound(cursorOperator, new VersionstampVal(lastVersionstamp));

        ExecutionState state = ctx.getOrCreateExecutionState(nodeId);
        if (ctx.isReverse()) {
            // For reverse scans: set upper bound (LT) to continue before the last processed
            state.setUpper(cursorBound);
            state.setLower(null);
        } else {
            // For forward scans: set lower bound (GT) to continue after last processed
            state.setLower(cursorBound);
            state.setUpper(null);
        }
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
     * Retrieves the last processed cursor position for the specified index definition,
     * based on the bounds stored in the provided plan executor configuration.
     * If no bounds or versionstamp information is available, this method returns null.
     *
     * @return the last processed cursor position as a {@code CursorPosition} object, or null if unavailable
     */
    CursorPosition getLastProcessedPosition(ExecutionState state, int nodeId) {
        // Check both lower and upper bounds for cursor position info
        Bound cursorBound = state.getLower() != null ? state.getLower() : state.getUpper();
        if (cursorBound != null && cursorBound.versionstamp() != null) {
            return new CursorPosition(nodeId, cursorBound.value(), cursorBound.versionstamp());
        }
        return null;
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
     * Gets the last processed index value and versionstamp for a definition from stored cursor bounds.
     * Returns null if no cursor bounds exist or they don't contain precise positioning info.
     */
    record CursorPosition(int nodeId, BqlValue indexValue, Versionstamp versionstamp) {
    }
}