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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.*;

public class CursorManager {

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
    CursorPosition getLastProcessedPosition(Cursor cursor, int nodeId) {
        // Check both lower and upper bounds for cursor position info
        Bound cursorBound = cursor.bounds().lower() != null ? cursor.bounds().lower() : cursor.bounds().upper();
        if (cursorBound != null && cursorBound.versionstamp() != null) {
            return new CursorPosition(nodeId, cursorBound.value(), cursorBound.versionstamp());
        }
        return null;
    }

    /**
     * Gets the last processed index value and versionstamp for a definition from stored cursor bounds.
     * Returns null if no cursor bounds exist or they don't contain precise positioning info.
     */
    record CursorPosition(int nodeId, BqlValue indexValue, Versionstamp versionstamp) {
    }
}