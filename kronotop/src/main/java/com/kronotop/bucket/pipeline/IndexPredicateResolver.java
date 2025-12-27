/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.kronotop.bucket.bql.ast.BinaryVal;
import com.kronotop.bucket.bql.ast.BooleanVal;
import com.kronotop.bucket.bql.ast.DateTimeVal;
import com.kronotop.bucket.bql.ast.Decimal128Val;
import com.kronotop.bucket.bql.ast.DoubleVal;
import com.kronotop.bucket.bql.ast.Int32Val;
import com.kronotop.bucket.bql.ast.Int64Val;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.bql.ast.TimestampVal;
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

import java.math.BigDecimal;

/**
 * Converts BQL AST values to BSON values for index key lookups.
 * <p>
 * Handles type-safe conversion between query predicate values and their corresponding
 * BSON representations. Returns null when the predicate value type doesn't match the
 * index's declared BSON type, indicating the index cannot be used for this predicate.
 */
class IndexPredicateResolver {

    /**
     * Converts a single predicate operand to a BSON value for index scan.
     *
     * @param def  the index definition specifying the expected BSON type
     * @param node the index scan node containing the predicate
     * @return the converted BsonValue, or null if types don't match
     */
    static BsonValue resolveIndexKeyValue(IndexDefinition def, IndexScanNode node) {
        return switch (def.bsonType()) {
            case INT32 -> {
                if (node.predicate().operand() instanceof Int32Val(int v)) {
                    yield new BsonInt32(v);
                }
                yield null;
            }
            case INT64 -> {
                if (node.predicate().operand() instanceof Int64Val(long v)) {
                    yield new BsonInt64(v);
                }
                yield null;
            }
            case STRING -> {
                if (node.predicate().operand() instanceof StringVal(String v)) {
                    yield new BsonString(v);
                }
                yield null;
            }
            case DOUBLE -> {
                if (node.predicate().operand() instanceof DoubleVal(double v)) {
                    yield new BsonDouble(v);
                }
                yield null;
            }
            case BINARY -> {
                if (node.predicate().operand() instanceof BinaryVal(byte[] v)) {
                    yield new BsonBinary(v);
                }
                yield null;
            }
            case BOOLEAN -> {
                if (node.predicate().operand() instanceof BooleanVal(boolean v)) {
                    yield new BsonBoolean(v);
                }
                yield null;
            }
            case DATE_TIME -> {
                if (node.predicate().operand() instanceof DateTimeVal(long v)) {
                    yield new BsonDateTime(v);
                }
                yield null;
            }
            case TIMESTAMP -> {
                if (node.predicate().operand() instanceof TimestampVal(long v)) {
                    yield new BsonTimestamp(v);
                }
                yield null;
            }
            case DECIMAL128 -> {
                if (node.predicate().operand() instanceof Decimal128Val(BigDecimal v)) {
                    yield new BsonDecimal128(new Decimal128(v));
                }
                yield null;
            }
            default -> null;
        };
    }

    /**
     * Converts range predicate bounds to BSON values for range scan.
     *
     * @param def  the index definition specifying the expected BSON type
     * @param node the range scan node containing lower and upper bounds
     * @return the converted IndexKeyRange, or null if types don't match
     */
    static IndexKeyRange resolveIndexKeyRange(IndexDefinition def, RangeScanNode node) {
        return switch (def.bsonType()) {
            case INT32 -> {
                if (node.predicate().lowerBound() instanceof Int32Val(int l) &&
                        node.predicate().upperBound() instanceof Int32Val(int u)) {
                    yield new IndexKeyRange(new BsonInt32(l), new BsonInt32(u));
                }
                yield null;
            }
            case INT64 -> {
                if (node.predicate().lowerBound() instanceof Int64Val(long l) &&
                        node.predicate().upperBound() instanceof Int64Val(long u)) {
                    yield new IndexKeyRange(new BsonInt64(l), new BsonInt64(u));
                }
                yield null;
            }
            case STRING -> {
                if (node.predicate().lowerBound() instanceof StringVal(String l) &&
                        node.predicate().upperBound() instanceof StringVal(String u)) {
                    yield new IndexKeyRange(new BsonString(l), new BsonString(u));
                }
                yield null;
            }
            case DOUBLE -> {
                if (node.predicate().lowerBound() instanceof DoubleVal(double l) &&
                        node.predicate().upperBound() instanceof DoubleVal(double u)) {
                    yield new IndexKeyRange(new BsonDouble(l), new BsonDouble(u));
                }
                yield null;
            }
            case BINARY -> {
                if (node.predicate().lowerBound() instanceof BinaryVal(byte[] l) &&
                        node.predicate().upperBound() instanceof BinaryVal(byte[] u)) {
                    yield new IndexKeyRange(new BsonBinary(l), new BsonBinary(u));
                }
                yield null;
            }
            case BOOLEAN -> {
                if (node.predicate().lowerBound() instanceof BooleanVal(boolean l) &&
                        node.predicate().upperBound() instanceof BooleanVal(boolean u)) {
                    yield new IndexKeyRange(new BsonBoolean(l), new BsonBoolean(u));
                }
                yield null;
            }
            case DATE_TIME -> {
                if (node.predicate().lowerBound() instanceof DateTimeVal(long l) &&
                        node.predicate().upperBound() instanceof DateTimeVal(long u)) {
                    yield new IndexKeyRange(new BsonDateTime(l), new BsonDateTime(u));
                }
                yield null;
            }
            case TIMESTAMP -> {
                if (node.predicate().lowerBound() instanceof TimestampVal(long l) &&
                        node.predicate().upperBound() instanceof TimestampVal(long u)) {
                    yield new IndexKeyRange(new BsonTimestamp(l), new BsonTimestamp(u));
                }
                yield null;
            }
            case DECIMAL128 -> {
                if (node.predicate().lowerBound() instanceof Decimal128Val(BigDecimal l) &&
                        node.predicate().upperBound() instanceof Decimal128Val(BigDecimal u)) {
                    yield new IndexKeyRange(new BsonDecimal128(new Decimal128(l)), new BsonDecimal128(new Decimal128(u)));
                }
                yield null;
            }
            default -> null;
        };
    }

    /**
     * Holds the lower and upper BSON values for a range scan.
     */
    protected record IndexKeyRange(BsonValue lower, BsonValue upper) {
    }
}
