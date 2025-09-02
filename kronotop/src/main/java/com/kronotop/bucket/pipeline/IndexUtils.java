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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSubspaceMagic;

import java.math.BigDecimal;

public class IndexUtils {

    /**
     * Extracts the raw value from a {@link Bound}'s value based on its type. The method handles
     * conversion for specific {@link BqlValue} types and throws an exception for unsupported types.
     *
     * @param bound the {@link Bound} object containing the value to extract
     * @return the extracted value of the bound, cast to the appropriate type
     * @throws IllegalStateException if the value type is unsupported
     */
    private static Object extractBoundValue(Bound bound) {
        Object boundValue = bound.value();

        // Note: VersionstampVal bounds are handled by the precise cursor logic using static maps
        // This method now only handles regular BqlValue bounds for non-cursor operations

        // Handle regular BqlValue bounds
        boundValue = switch (boundValue) {
            case StringVal(String value) -> value;
            case Int32Val(int value) -> (long) value;
            case Int64Val(long value) -> value;
            case DoubleVal(double value) -> value;
            case BooleanVal(boolean value) -> value;
            default ->
                    throw new IllegalStateException("Unsupported bound value type: " + boundValue.getClass().getSimpleName());
        };
        return boundValue;
    }

    /**
     * Constructs a {@link KeySelector} based on the specified bound and its value.
     * The method computes a key for index navigation and determines the appropriate selector
     * for the provided bound operator (GT, GTE, LT, LTE).
     *
     * @param indexSubspace the {@link DirectorySubspace} representing the index subspace
     * @param bound         a {@link Bound} object representing the cursor's boundary operator and value
     * @param boundValue    the value used to construct the boundary key for the index
     * @return a {@link KeySelector} configured for the specified boundary and operator
     * @throws IllegalStateException if the {@link Bound#operator()} is unsupported
     */
    static KeySelector getKeySelector(DirectorySubspace indexSubspace, Bound bound, Object boundValue) {
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), boundValue);
        byte[] boundKey = indexSubspace.pack(tuple);

        return getKeySelector(bound, boundKey);
    }

    private static KeySelector getKeySelector(Bound bound, byte[] boundKey) {
        return switch (bound.operator()) {
            case GT, LTE -> KeySelector.firstGreaterThan(boundKey);
            case GTE, LT -> KeySelector.firstGreaterOrEqual(boundKey);
            default -> throw new IllegalStateException("Unsupported cursor operator: " + bound.operator());
        };
    }

    /**
     * Creates index entries prefix for scanning.
     */
    byte[] createIndexEntriesPrefix(DirectorySubspace indexSubspace) {
        return indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
    }

    /**
     * Creates index entries boundary for scanning.
     */
    byte[] createIndexEntriesBoundary(DirectorySubspace indexSubspace) {
        byte[] prefix = createIndexEntriesPrefix(indexSubspace);
        return ByteArrayUtil.strinc(prefix);
    }

    /**
     * Constructs index range selectors for a given filter with index definition.
     */
    KeySelector[] constructIndexRangeSelectors(DirectorySubspace indexSubspace, IndexScanPredicate predicate, IndexDefinition definition) {
        Object indexValue;
        Tuple indexTuple;

        // Special handling for _id index - it uses Versionstamp directly, not bytes
        if (definition != null && DefaultIndexDefinition.ID.selector().equals(definition.selector()) && predicate.operand() instanceof VersionstampVal(
                Versionstamp value
        )) {
            indexValue = value; // Use Versionstamp directly
        } else {
            indexValue = extractIndexValue(predicate.operand());
        }

        indexTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue);

        byte[] indexKey = indexSubspace.pack(indexTuple);

        return switch (predicate.op()) {
            case EQ -> {
                // For equality, scan all entries with the same indexed value
                // The key structure is [ENTRIES_MAGIC, indexed_value, versionstamp]
                // We need to scan all entries that start with [ENTRIES_MAGIC, indexed_value]
                KeySelector begin = KeySelector.firstGreaterOrEqual(indexKey);
                byte[] endKey = ByteArrayUtil.strinc(indexKey);
                KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
                yield new KeySelector[]{begin, end};
            }
            case GT -> {
                // Greater than: start after all entries with this indexed value
                byte[] endKey = ByteArrayUtil.strinc(indexKey);
                KeySelector begin = KeySelector.firstGreaterOrEqual(endKey);
                KeySelector end = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(indexSubspace));
                yield new KeySelector[]{begin, end};
            }
            case GTE -> {
                // Greater than or equal: start at the key, end at the index boundary
                KeySelector begin = KeySelector.firstGreaterOrEqual(indexKey);
                KeySelector end = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(indexSubspace));
                yield new KeySelector[]{begin, end};
            }
            case LT -> {
                // Less than: start at the index beginning, end before the key
                KeySelector begin = KeySelector.firstGreaterOrEqual(createIndexEntriesPrefix(indexSubspace));
                KeySelector end = KeySelector.firstGreaterOrEqual(indexKey);
                yield new KeySelector[]{begin, end};
            }
            case LTE -> {
                // Less than or equal: start at the index beginning, end after all entries with this value
                KeySelector begin = KeySelector.firstGreaterOrEqual(createIndexEntriesPrefix(indexSubspace));
                byte[] endKey = ByteArrayUtil.strinc(indexKey);
                KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
                yield new KeySelector[]{begin, end};
            }
            case NE -> {
                // Not equal: this would require multiple ranges, fallback to full scan with filter
                KeySelector begin = KeySelector.firstGreaterOrEqual(createIndexEntriesPrefix(indexSubspace));
                KeySelector end = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(indexSubspace));
                yield new KeySelector[]{begin, end};
            }
            default ->
                    throw new UnsupportedOperationException("Index scan not supported for operator: " + predicate.op());
        };
    }

    /**
     * Extracts the raw value from a BqlValue for index operations.
     */
    Object extractIndexValue(Object operand) {
        return switch (operand) {
            case StringVal(String value) -> value;
            case Int32Val(int value) -> (long) value; // Index stores INT32 as long
            case Int64Val(long value) -> value;
            case DoubleVal(double value) -> value;
            case BooleanVal(boolean value) -> value;
            case BinaryVal(byte[] value) -> value;
            case DateTimeVal(long value) -> value;
            case TimestampVal(long value) -> value;
            case Decimal128Val(BigDecimal value) -> value;
            case NullVal ignored -> null;
            case VersionstampVal(Versionstamp value) -> value;
            default ->
                    throw new IllegalArgumentException("Unsupported operand type for index: " + operand.getClass().getSimpleName());
        };
    }

    /**
     * Creates a KeySelector from a cursor bound for index pagination.
     */
    KeySelector createIndexSelectorFromBound(DirectorySubspace indexSubspace, Bound bound) {
        if (bound == null) {
            throw new IllegalArgumentException("Bound cannot be null");
        }

        // For secondary indexes with a versionstamp set, create a precise key with both value and versionstamp
        if (bound.versionstamp() != null) {
            // Secondary Index Key Structure -> BqlValue | Versionstamp
            Object indexValue = extractBoundValue(bound);
            Tuple indexTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue, bound.versionstamp());
            byte[] indexKey = indexSubspace.pack(indexTuple);

            return getKeySelector(bound, indexKey);
        } else {
            // Primary index or secondary index without a precise versionstamp
            Object boundValue = extractBoundValue(bound);
            return getKeySelector(indexSubspace, bound, boundValue);
        }
    }
}