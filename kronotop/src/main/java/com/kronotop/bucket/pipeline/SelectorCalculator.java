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
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.KronotopException;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.bucket.NumericWidening;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.IndexMaintainer;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.PrimaryIndex;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.Operator;
import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.util.List;

import static com.kronotop.bucket.pipeline.IndexUtil.getKeySelector;

/**
 * Calculates FoundationDB KeySelector pairs for different types of scan operations in the query pipeline.
 *
 * <p>This class is responsible for determining the appropriate start and end positions for scanning
 * FoundationDB index entries based on the scan context and cursor state. It supports three main
 * scan types:</p>
 *
 * <ul>
 *   <li><strong>Full Scan</strong> - Scans all entries in an index without filtering</li>
 *   <li><strong>Index Scan</strong> - Scans index entries matching a specific condition (=, >, >=, <, <=, !=)</li>
 *   <li><strong>Range Scan</strong> - Scans index entries within a specified range with inclusive/exclusive bounds</li>
 * </ul>
 *
 * <p><strong>Key Structure:</strong><br>
 * All index entries follow the structure: {@code [ENTRIES_MAGIC, indexed_value, objectId]}<br>
 * This allows for efficient range queries and cursor-based pagination.</p>
 *
 * <p><strong>Cursor Support:</strong><br>
 * The calculator handles continuation from previous scan operations by examining the cursor state
 * and constructing selectors that resume from the last processed position while maintaining
 * scan direction and boundary constraints.</p>
 *
 * <p><strong>Direction Support:</strong><br>
 * Both forward and reverse scans are supported. Reverse scans require special handling of
 * selector boundaries to ensure correct ordering and continuation.</p>
 *
 * @since 0.13
 */
class SelectorCalculator {
    private static final CursorManager cursorManager = new CursorManager();

    /**
     * Calculates the appropriate KeySelector pair for the given scan context.
     *
     * <p>This is the main entry point that dispatches to specific calculation methods
     * based on the scan context type. Each scan type requires different logic for
     * determining start and end positions.</p>
     *
     * @param context the scan context containing operation details, cursor state, and predicates
     * @return a SelectorPair containing begin and end KeySelectors for the scan operation
     * @throws KronotopException if the context type is not supported
     */
    static SelectorPair calculate(ScanContext context) {
        return switch (context) {
            case FullScanContext ctx -> calculateFullScanSelectors(ctx);
            case IndexScanContext ctx -> calculateIndexScanSelectors(ctx);
            case RangeScanContext ctx -> calculateRangeScanSelectors(ctx);
            default -> throw new KronotopException("unknown context type");
        };
    }

    /**
     * Calculates KeySelector pair for range scan operations with upper and lower bounds.
     *
     * <p>Range scans support both inclusive and exclusive bounds on both ends of the range.
     * When continuing from a cursor position, the method ensures that the scan resumes from
     * the correct position while maintaining the original range boundaries.</p>
     *
     * <p><strong>Fresh Scan:</strong> Constructs selectors based solely on the range predicate bounds.</p>
     * <p><strong>Continuation:</strong> Adjusts the appropriate selector (begin for forward, end for reverse)
     * to resume from the cursor position while preserving the opposite boundary.</p>
     *
     * @param ctx the range scan context containing bounds, direction, and cursor state
     * @return a SelectorPair for scanning the specified range
     */
    private static SelectorPair calculateRangeScanSelectors(RangeScanContext ctx) {
        if (ctx.state().isEmpty()) {
            return constructRangeScanSelectors(
                    ctx.indexSubspace(),
                    ctx.predicate(),
                    ctx.getParameters(),
                    ctx.index(),
                    ctx.collation(),
                    ctx.collatorCache()
            );
        }
        return createContinuationSelectors(
                ctx.state(),
                ctx.nodeId(),
                ctx.getSortDirection(),
                ctx.indexSubspace(),
                ctx.isReverse(),
                ctx.index()
        );
    }

    /**
     * Builds the cursor tuple based on the index type.
     * Primary index: (ENTRIES, objectId)
     * Secondary index: (ENTRIES, value, objectId)
     */
    private static Tuple buildCursorTuple(SingleFieldIndexDefinition definition, Object indexValue, ObjectId objectId) {
        if (PrimaryIndex.isPrimary(definition)) {
            // Primary index: key structure is (ENTRIES, objectId)
            return Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue);
        }
        // Secondary index: key structure is (ENTRIES, value, objectId)
        return Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue, objectId.toByteArray());
    }

    /**
     * Creates continuation selectors for resuming a scan from a cursor position.
     * For reverse scans, adjusts the end selector while keeping the begin unchanged.
     * For forward scans, adjusts the begin selector while keeping the end unchanged.
     */
    private static SelectorPair createContinuationSelectors(
            ExecutionState state,
            int nodeId,
            SortDirection sortDirection,
            DirectorySubspace indexSubspace,
            boolean isReverse,
            SingleFieldIndexDefinition definition
    ) {
        CursorManager.CursorPosition position = cursorManager.getLastProcessedPosition(state, nodeId, sortDirection);
        Object indexValue = extractIndexValueFromBqlValue(position.indexValue());
        Tuple cursorTuple = buildCursorTuple(definition, indexValue, position.objectId());
        byte[] cursorKeyBytes = indexSubspace.pack(cursorTuple);

        if (isReverse) {
            return new SelectorPair(state.getSelector().begin(), KeySelector.firstGreaterOrEqual(cursorKeyBytes));
        }
        return new SelectorPair(KeySelector.firstGreaterThan(cursorKeyBytes), state.getSelector().end());
    }

    /**
     * Calculates KeySelector pair for full scan operations across an entire index.
     *
     * <p>Full scans traverse all entries in an index without any filtering predicates.
     * The method handles both forward and reverse scans, with cursor-based continuation
     * support for paginated results.</p>
     *
     * <p><strong>Forward Scans:</strong> Start from the index beginning and scan towards the end.
     * For continuations, resume from the lower bound in the cursor state.</p>
     *
     * <p><strong>Reverse Scans:</strong> Start from the index end and scan towards the beginning.
     * For continuations, resume from the upper bound in the cursor state.</p>
     *
     * @param ctx the full scan context containing direction and cursor state
     * @return a SelectorPair for scanning the entire index
     */
    private static SelectorPair calculateFullScanSelectors(FullScanContext ctx) {
        byte[] basePrefix = createIndexEntriesPrefix(ctx.indexSubspace());
        KeySelector beginSelector;
        KeySelector endSelector;

        boolean isReverse = ctx.isReverse();
        boolean isInitialScan = ctx.state().isEmpty();

        if (isReverse) {
            // Reverse scan: Start from latest, move backwards
            beginSelector = KeySelector.firstGreaterOrEqual(basePrefix);

            if (isInitialScan) {
                endSelector = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(ctx.indexSubspace()));
            } else {
                if (ctx.state().getUpper() != null) {
                    endSelector = cursorManager.createSelectorFromBound(ctx.indexSubspace(), ctx.state().getUpper());
                } else {
                    endSelector = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(ctx.indexSubspace()));
                }
            }
        } else {
            // Forward scan: Start from beginning, move forward
            beginSelector = isInitialScan
                    ? KeySelector.firstGreaterOrEqual(basePrefix)
                    : createSelectorFromBound(ctx.indexSubspace(), ctx.state().getLower());

            endSelector = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(ctx.indexSubspace()));
        }

        return new SelectorPair(beginSelector, endSelector);
    }

    /**
     * Calculates KeySelector pair for index scan operations with specific filter conditions.
     *
     * <p>Index scans apply single-operand predicates (=, >, >=, <, <=, !=) to filter index
     * entries before retrieving documents. This method translates logical operators into
     * appropriate FoundationDB KeySelector ranges.</p>
     *
     * <p><strong>Fresh Scan:</strong> Constructs selectors based on the operator and operand value.</p>
     * <p><strong>Continuation:</strong> Adjusts the appropriate selector boundary to resume from
     * the cursor position while maintaining the original filter constraints.</p>
     *
     * <p><strong>Supported Operators:</strong></p>
     * <ul>
     *   <li><strong>EQ</strong> - Scans all entries with exactly the specified value</li>
     *   <li><strong>GT/GTE</strong> - Scans from the value boundary to index end</li>
     *   <li><strong>LT/LTE</strong> - Scans from index beginning to the value boundary</li>
     *   <li><strong>NE</strong> - Falls back to full scan with filtering (TODO: optimize with multiple ranges)</li>
     * </ul>
     *
     * @param ctx the index scan context containing predicate, direction, and cursor state
     * @return a SelectorPair for scanning entries matching the filter condition
     */
    private static SelectorPair calculateIndexScanSelectors(IndexScanContext ctx) {
        if (ctx.state().isEmpty()) {
            BqlValue resolvedOperand = ctx.predicate().operand().resolve(ctx.getParameters());
            return constructSelectorsForIndexScan(
                    ctx.indexSubspace(),
                    ctx.predicate().op(),
                    resolvedOperand,
                    ctx.index(),
                    ctx.collation(),
                    ctx.collatorCache());
        }
        return createContinuationSelectors(
                ctx.state(),
                ctx.nodeId(),
                ctx.getSortDirection(),
                ctx.indexSubspace(),
                ctx.isReverse(),
                ctx.index()
        );
    }

    /**
     * Constructs KeySelector pair for fresh index scan operations based on operator and operand.
     *
     * <p>This method translates BQL operators into appropriate FoundationDB KeySelector ranges
     * and provides optimized range construction for each operator type.</p>
     *
     * <p><strong>Key Structure:</strong> {@code [ENTRIES_MAGIC, indexed_value, objectId]}</p>
     *
     * <p><strong>Operator Handling:</strong></p>
     * <ul>
     *   <li><strong>EQ:</strong> Scans entries where {@code indexed_value} equals the operand</li>
     *   <li><strong>GT:</strong> Scans entries where {@code indexed_value > operand}</li>
     *   <li><strong>GTE:</strong> Scans entries where {@code indexed_value >= operand}</li>
     *   <li><strong>LT:</strong> Scans entries where {@code indexed_value < operand}</li>
     *   <li><strong>LTE:</strong> Scans entries where {@code indexed_value <= operand}</li>
     *   <li><strong>NE:</strong> Falls back to full scan (optimization opportunity for multiple ranges)</li>
     * </ul>
     *
     * @param indexSubspace the FoundationDB subspace for this index
     * @param operator      the comparison operator (EQ, GT, GTE, LT, LTE, NE)
     * @param operand       the value to compare against (BqlValue)
     * @param definition    the index definition for special handling
     * @return a SelectorPair defining the scan range for this operation
     * @throws UnsupportedOperationException if the operator is not supported for index scans
     */
    private static SelectorPair constructSelectorsForIndexScan(DirectorySubspace indexSubspace, Operator operator, Object operand,
                                                               SingleFieldIndexDefinition definition, Collation collation, CollatorCache collatorCache) {
        Object indexValue = extractIndexValueFromBqlValue((BqlValue) operand, definition.bsonType());
        indexValue = IndexMaintainer.applyCollation(indexValue, collation, collatorCache);
        Tuple indexTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue);

        byte[] indexKey = indexSubspace.pack(indexTuple);

        return switch (operator) {
            case EQ -> {
                // For equality, scan all entries with the same indexed value
                // The key structure is [ENTRIES_MAGIC, indexed_value, objectId]
                // We need to scan all entries that start with [ENTRIES_MAGIC, indexed_value]
                KeySelector begin = KeySelector.firstGreaterOrEqual(indexKey);
                byte[] endKey = ByteArrayUtil.strinc(indexKey);
                KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
                yield new SelectorPair(begin, end);
            }
            case GT -> {
                // Greater than: start after all entries with this indexed value
                byte[] endKey = ByteArrayUtil.strinc(indexKey);
                KeySelector begin = KeySelector.firstGreaterOrEqual(endKey);
                KeySelector end = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(indexSubspace));
                yield new SelectorPair(begin, end);
            }
            case GTE -> {
                // Greater than or equal: start at the key, end at the index boundary
                KeySelector begin = KeySelector.firstGreaterOrEqual(indexKey);
                KeySelector end = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(indexSubspace));
                yield new SelectorPair(begin, end);
            }
            case LT -> {
                // Less than: start at the index beginning, end before the key
                KeySelector begin = KeySelector.firstGreaterOrEqual(createIndexEntriesPrefix(indexSubspace));
                KeySelector end = KeySelector.firstGreaterOrEqual(indexKey);
                yield new SelectorPair(begin, end);
            }
            case LTE -> {
                // Less than or equal: start at the index beginning, end after all entries with this value
                KeySelector begin = KeySelector.firstGreaterOrEqual(createIndexEntriesPrefix(indexSubspace));
                byte[] endKey = ByteArrayUtil.strinc(indexKey);
                KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
                yield new SelectorPair(begin, end);
            }
            case NE -> {
                // Not equal: this would require multiple ranges, fallback to full scan with filter
                KeySelector begin = KeySelector.firstGreaterOrEqual(createIndexEntriesPrefix(indexSubspace));
                KeySelector end = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(indexSubspace));
                yield new SelectorPair(begin, end);
            }
            default -> throw new UnsupportedOperationException("Index scan not supported for operator: " + operator);
        };
    }

    /**
     * Creates index entries prefix for scanning.
     */
    private static byte[] createIndexEntriesPrefix(DirectorySubspace indexSubspace) {
        return indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
    }

    /**
     * Creates index entries boundary for scanning.
     */
    private static byte[] createIndexEntriesBoundary(DirectorySubspace indexSubspace) {
        byte[] prefix = createIndexEntriesPrefix(indexSubspace);
        return ByteArrayUtil.strinc(prefix);
    }

    /**
     * Extracts the native Java object from a BqlValue for index storage.
     *
     * <p>This method converts BQL value types into their corresponding Java objects that
     * can be stored in FoundationDB index entries. It handles type normalization (e.g.,
     * converting INT32 to long) to ensure consistent index storage.</p>
     *
     * <p><strong>Type Mapping:</strong></p>
     * <ul>
     *   <li>StringVal → String</li>
     *   <li>Int32Val → Long (normalized for consistent storage)</li>
     *   <li>Int64Val → Long</li>
     *   <li>DoubleVal → Double</li>
     *   <li>BooleanVal → Boolean</li>
     *   <li>DateTimeVal → DateTime object</li>
     *   <li>TimestampVal → Timestamp object</li>
     *   <li>Decimal128Val → Decimal128 object</li>
     *   <li>BinaryVal → byte array</li>
     *   <li>ObjectIdVal → byte[] (ObjectId bytes for consistent index storage)</li>
     *   <li>NullVal → null</li>
     * </ul>
     *
     * @param bqlValue the BQL value to extract from
     * @return the native Java object for index storage
     * @throws IllegalArgumentException if the BqlValue type is not supported for indexing
     */
    static Object extractIndexValueFromBqlValue(BqlValue bqlValue) {
        return switch (bqlValue) {
            case StringVal stringVal -> stringVal.value();
            case Int32Val int32Val -> (long) int32Val.value(); // Store INT32 as long in index
            case Int64Val int64Val -> int64Val.value();
            case DoubleVal doubleVal -> doubleVal.value();
            case BooleanVal booleanVal -> booleanVal.value();
            case DateTimeVal dateTimeVal -> dateTimeVal.value();
            case TimestampVal timestampVal -> timestampVal.value();
            case Decimal128Val decimal128Val -> decimal128Val.value();
            case BinaryVal binaryVal -> binaryVal.value();
            case ObjectIdVal objectIdVal -> objectIdVal.value().toByteArray();
            case NullVal ignored -> null;
            default ->
                    throw new IllegalArgumentException("Unsupported BqlValue type for index: " + bqlValue.getClass().getSimpleName());
        };
    }

    /**
     * Extracts a Java object from a BqlValue, applying lossless numeric widening to match
     * the index field's target type. This ensures FDB tuple encoding matches what was stored
     * at index build time.
     *
     * @param bqlValue   the BQL value to extract from
     * @param targetType the index field's declared BSON type
     * @return the native Java object for FDB tuple packing
     */
    static Object extractIndexValueFromBqlValue(BqlValue bqlValue, BsonType targetType) {
        BsonValue bsonValue = bqlValueToBsonValue(bqlValue);
        if (bsonValue != null && NumericWidening.isNumericBsonType(bsonValue.getBsonType())) {
            Object widened = NumericWidening.widenValue(bsonValue, targetType);
            if (widened != null) {
                return widened;
            }
        }
        return extractIndexValueFromBqlValue(bqlValue);
    }

    private static BsonValue bqlValueToBsonValue(BqlValue bqlValue) {
        return switch (bqlValue) {
            case Int32Val(int v) -> new BsonInt32(v);
            case Int64Val(long v) -> new BsonInt64(v);
            case DoubleVal(double v) -> new BsonDouble(v);
            case Decimal128Val(java.math.BigDecimal v) -> new BsonDecimal128(new Decimal128(v));
            default -> null;
        };
    }

    /**
     * Creates a KeySelector from a cursor bound for _id index continuation.
     *
     * <p>This method is specifically designed for the _id index (primary index) which uses
     * ObjectId values. It extracts the ObjectId from the bound and constructs
     * the appropriate KeySelector.</p>
     *
     * <p><strong>Note:</strong> This method assumes the bound contains an ObjectIdVal,
     * which is appropriate since it's used for _id index operations where ObjectIds
     * are the primary key.</p>
     *
     * @param idIndexSubspace the _id index subspace
     * @param bound           the cursor bound containing position information
     * @return a KeySelector for resuming from the bound position
     * @throws IllegalArgumentException if bound is null
     * @throws IllegalStateException    if bound value is not an ObjectIdVal
     */
    private static KeySelector createSelectorFromBound(DirectorySubspace idIndexSubspace, Bound bound) {
        if (bound == null) {
            throw new IllegalArgumentException("Bound cannot be null");
        }

        Object boundValue = bound.value();
        if (boundValue instanceof ObjectIdVal(ObjectId value)) {
            boundValue = value.toByteArray(); // Convert ObjectId to a byte array for tuple packing
        } else {
            throw new IllegalStateException("Bound value must be ObjectIdVal, got: " + boundValue.getClass().getSimpleName());
        }

        return getKeySelector(idIndexSubspace, bound, boundValue);
    }

    /**
     * Constructs KeySelector pair for fresh range scan operations.
     *
     * <p>This method creates begin and end selectors for range scans based on the predicate's
     * upper and lower bounds. Both bounds are optional, and each can be inclusive or exclusive.</p>
     *
     * <p><strong>Lower Bound Handling:</strong></p>
     * <ul>
     *   <li>If present and inclusive: {@code firstGreaterOrEqual(lowerKey)}</li>
     *   <li>If present and exclusive: {@code firstGreaterThan(strinc(lowerKey))}</li>
     *   <li>If absent: Start from index beginning</li>
     * </ul>
     *
     * <p><strong>Upper Bound Handling:</strong></p>
     * <ul>
     *   <li>If present and inclusive: {@code firstGreaterOrEqual(strinc(upperKey))} to include all entries with that value</li>
     *   <li>If present and exclusive: {@code firstGreaterOrEqual(upperKey)} to exclude entries with that value</li>
     *   <li>If absent: End at index boundary</li>
     * </ul>
     *
     * @param indexSubspace the FoundationDB subspace for this index
     * @param predicate     the range scan predicate containing bounds and inclusion flags
     * @param parameters    the parameter list for resolving Param operands
     * @return a SelectorPair defining the range scan boundaries
     */
    private static SelectorPair constructRangeScanSelectors(DirectorySubspace indexSubspace, RangeScanPredicate predicate,
                                                            List<BqlValue> parameters, SingleFieldIndexDefinition index,
                                                            Collation collation, CollatorCache collatorCache) {
        KeySelector beginSelector;
        KeySelector endSelector;

        BqlValue lowerBound = predicate.resolveLowerBound(parameters);
        BqlValue upperBound = predicate.resolveUpperBound(parameters);

        // Handle lower bound
        if (lowerBound != null) {
            Object lowerIndexValue = extractIndexValueFromBqlValue(lowerBound, index.bsonType());
            lowerIndexValue = IndexMaintainer.applyCollation(lowerIndexValue, collation, collatorCache);
            Tuple lowerTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), lowerIndexValue);
            byte[] lowerKey = indexSubspace.pack(lowerTuple);

            if (predicate.includeLower()) {
                beginSelector = KeySelector.firstGreaterOrEqual(lowerKey);
            } else {
                byte[] endKey = ByteArrayUtil.strinc(lowerKey);
                beginSelector = KeySelector.firstGreaterThan(endKey);
            }
        } else {
            beginSelector = KeySelector.firstGreaterOrEqual(createIndexEntriesPrefix(indexSubspace));
        }

        // Handle upper bound
        if (upperBound != null) {
            Object upperIndexValue = extractIndexValueFromBqlValue(upperBound, index.bsonType());
            upperIndexValue = IndexMaintainer.applyCollation(upperIndexValue, collation, collatorCache);
            Tuple upperTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), upperIndexValue);
            byte[] upperKey = indexSubspace.pack(upperTuple);

            if (predicate.includeUpper()) {
                // Include upper bound: scan up to and including all entries with this value
                // Include all entries with this value (same logic as LTE)
                byte[] endKey = ByteArrayUtil.strinc(upperKey);
                endSelector = KeySelector.firstGreaterOrEqual(endKey);
            } else {
                // Exclude upper bound: scan up to but not including entries with this value
                endSelector = KeySelector.firstGreaterOrEqual(upperKey);
            }
        } else {
            endSelector = KeySelector.firstGreaterOrEqual(createIndexEntriesBoundary(indexSubspace));
        }

        return new SelectorPair(beginSelector, endSelector);
    }
}
