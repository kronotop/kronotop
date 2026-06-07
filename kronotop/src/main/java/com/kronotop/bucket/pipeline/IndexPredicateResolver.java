/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.util.List;

/**
 * Converts BQL AST values to BSON values for index key lookups.
 * <p>
 * Handles type-safe conversion between query predicate values and their corresponding
 * BSON representations. Supports lossless numeric widening: if the predicate value type
 * differs from the index's declared type but can be losslessly widened, the value is
 * converted. Returns null when neither exact match nor widening is possible.
 */
class IndexPredicateResolver {

    /**
     * Converts a single predicate operand to a BSON value for index scan.
     *
     * @param def        the index definition specifying the expected BSON type
     * @param node       the index scan node containing the predicate
     * @param parameters the parameter list for resolving Param operands
     * @return the converted BsonValue, or null if types don't match
     */
    static BsonValue resolveIndexKeyValue(SingleFieldIndexDefinition def, IndexScanNode node, List<BqlValue> parameters) {
        BqlValue operand = node.predicate().operand().resolve(parameters);
        return switch (def.bsonType()) {
            case OBJECT_ID -> {
                if (operand instanceof ObjectIdVal(ObjectId v)) {
                    yield new BsonObjectId(v);
                }
                yield null;
            }
            case INT32 -> {
                if (operand instanceof Int32Val(int v)) {
                    yield new BsonInt32(v);
                }
                yield null;
            }
            case INT64 -> {
                if (operand instanceof Int64Val(long v)) {
                    yield new BsonInt64(v);
                }
                // Widening: INT32 -> INT64
                if (operand instanceof Int32Val(int v)) {
                    yield new BsonInt64(v);
                }
                yield null;
            }
            case STRING -> {
                if (operand instanceof StringVal(String v)) {
                    yield new BsonString(v);
                }
                yield null;
            }
            case DOUBLE -> {
                if (operand instanceof DoubleVal(double v)) {
                    yield new BsonDouble(v);
                }
                // Widening: INT32 -> DOUBLE
                if (operand instanceof Int32Val(int v)) {
                    yield new BsonDouble(v);
                }
                yield null;
            }
            case BINARY -> {
                if (operand instanceof BinaryVal(byte[] v)) {
                    yield new BsonBinary(v);
                }
                yield null;
            }
            case BOOLEAN -> {
                if (operand instanceof BooleanVal(boolean v)) {
                    yield new BsonBoolean(v);
                }
                yield null;
            }
            case DATE_TIME -> {
                if (operand instanceof DateTimeVal(long v)) {
                    yield new BsonDateTime(v);
                }
                yield null;
            }
            case TIMESTAMP -> {
                if (operand instanceof TimestampVal(long v)) {
                    yield new BsonTimestamp(v);
                }
                yield null;
            }
            case DECIMAL128 -> {
                if (operand instanceof Decimal128Val(BigDecimal v)) {
                    yield new BsonDecimal128(new Decimal128(v));
                }
                // Widening: INT32 -> DECIMAL128
                if (operand instanceof Int32Val(int v)) {
                    yield new BsonDecimal128(new Decimal128(BigDecimal.valueOf(v)));
                }
                // Widening: INT64 -> DECIMAL128
                if (operand instanceof Int64Val(long v)) {
                    yield new BsonDecimal128(new Decimal128(BigDecimal.valueOf(v)));
                }
                // Widening: DOUBLE -> DECIMAL128
                if (operand instanceof DoubleVal(double v)) {
                    yield new BsonDecimal128(new Decimal128(BigDecimal.valueOf(v)));
                }
                yield null;
            }
            default -> null;
        };
    }

    /**
     * Converts range predicate bounds to BSON values for range scan.
     *
     * @param def        the index definition specifying the expected BSON type
     * @param node       the range scan node containing lower and upper bounds
     * @param parameters the parameter list for resolving Param operands
     * @return the converted IndexKeyRange, or null if types don't match
     */
    static IndexKeyRange resolveIndexKeyRange(SingleFieldIndexDefinition def, RangeScanNode node, List<BqlValue> parameters) {
        BqlValue lowerBound = node.predicate().resolveLowerBound(parameters);
        BqlValue upperBound = node.predicate().resolveUpperBound(parameters);

        return switch (def.bsonType()) {
            case OBJECT_ID -> {
                if (lowerBound instanceof ObjectIdVal(ObjectId l) &&
                        upperBound instanceof ObjectIdVal(ObjectId u)) {
                    yield new IndexKeyRange(new BsonObjectId(l), new BsonObjectId(u));
                }
                yield null;
            }
            case INT32 -> {
                if (lowerBound instanceof Int32Val(int l) &&
                        upperBound instanceof Int32Val(int u)) {
                    yield new IndexKeyRange(new BsonInt32(l), new BsonInt32(u));
                }
                yield null;
            }
            case INT64 -> {
                if (lowerBound instanceof Int64Val(long l) &&
                        upperBound instanceof Int64Val(long u)) {
                    yield new IndexKeyRange(new BsonInt64(l), new BsonInt64(u));
                }
                // Widening: INT32 bounds -> INT64
                BsonValue lowerInt64 = widenBoundToInt64(lowerBound);
                BsonValue upperInt64 = widenBoundToInt64(upperBound);
                if (lowerInt64 != null && upperInt64 != null) {
                    yield new IndexKeyRange(lowerInt64, upperInt64);
                }
                yield null;
            }
            case STRING -> {
                if (lowerBound instanceof StringVal(String l) &&
                        upperBound instanceof StringVal(String u)) {
                    yield new IndexKeyRange(new BsonString(l), new BsonString(u));
                }
                yield null;
            }
            case DOUBLE -> {
                if (lowerBound instanceof DoubleVal(double l) &&
                        upperBound instanceof DoubleVal(double u)) {
                    yield new IndexKeyRange(new BsonDouble(l), new BsonDouble(u));
                }
                // Widening: INT32 bounds -> DOUBLE
                BsonValue lowerDouble = widenBoundToDouble(lowerBound);
                BsonValue upperDouble = widenBoundToDouble(upperBound);
                if (lowerDouble != null && upperDouble != null) {
                    yield new IndexKeyRange(lowerDouble, upperDouble);
                }
                yield null;
            }
            case BINARY -> {
                if (lowerBound instanceof BinaryVal(byte[] l) &&
                        upperBound instanceof BinaryVal(byte[] u)) {
                    yield new IndexKeyRange(new BsonBinary(l), new BsonBinary(u));
                }
                yield null;
            }
            case BOOLEAN -> {
                if (lowerBound instanceof BooleanVal(boolean l) &&
                        upperBound instanceof BooleanVal(boolean u)) {
                    yield new IndexKeyRange(new BsonBoolean(l), new BsonBoolean(u));
                }
                yield null;
            }
            case DATE_TIME -> {
                if (lowerBound instanceof DateTimeVal(long l) &&
                        upperBound instanceof DateTimeVal(long u)) {
                    yield new IndexKeyRange(new BsonDateTime(l), new BsonDateTime(u));
                }
                yield null;
            }
            case TIMESTAMP -> {
                if (lowerBound instanceof TimestampVal(long l) &&
                        upperBound instanceof TimestampVal(long u)) {
                    yield new IndexKeyRange(new BsonTimestamp(l), new BsonTimestamp(u));
                }
                yield null;
            }
            case DECIMAL128 -> {
                if (lowerBound instanceof Decimal128Val(BigDecimal l) &&
                        upperBound instanceof Decimal128Val(BigDecimal u)) {
                    yield new IndexKeyRange(new BsonDecimal128(new Decimal128(l)), new BsonDecimal128(new Decimal128(u)));
                }
                // Widening: INT32, INT64, DOUBLE bounds -> DECIMAL128
                BsonValue lowerDec = widenBoundToDecimal128(lowerBound);
                BsonValue upperDec = widenBoundToDecimal128(upperBound);
                if (lowerDec != null && upperDec != null) {
                    yield new IndexKeyRange(lowerDec, upperDec);
                }
                yield null;
            }
            default -> null;
        };
    }

    private static BsonValue widenBoundToInt64(BqlValue bound) {
        if (bound instanceof Int64Val(long v)) {
            return new BsonInt64(v);
        }
        if (bound instanceof Int32Val(int v)) {
            return new BsonInt64(v);
        }
        return null;
    }

    private static BsonValue widenBoundToDouble(BqlValue bound) {
        if (bound instanceof DoubleVal(double v)) {
            return new BsonDouble(v);
        }
        if (bound instanceof Int32Val(int v)) {
            return new BsonDouble(v);
        }
        return null;
    }

    private static BsonValue widenBoundToDecimal128(BqlValue bound) {
        if (bound instanceof Decimal128Val(BigDecimal v)) {
            return new BsonDecimal128(new Decimal128(v));
        }
        if (bound instanceof Int32Val(int v)) {
            return new BsonDecimal128(new Decimal128(BigDecimal.valueOf(v)));
        }
        if (bound instanceof Int64Val(long v)) {
            return new BsonDecimal128(new Decimal128(BigDecimal.valueOf(v)));
        }
        if (bound instanceof DoubleVal(double v)) {
            return new BsonDecimal128(new Decimal128(BigDecimal.valueOf(v)));
        }
        return null;
    }

    /**
     * Holds the lower and upper BSON values for a range scan.
     */
    protected record IndexKeyRange(BsonValue lower, BsonValue upper) {
    }
}
