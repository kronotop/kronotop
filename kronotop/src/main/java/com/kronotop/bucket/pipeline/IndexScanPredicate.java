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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.NumericWidening;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.Operator;
import org.bson.BsonType;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Predicate for index scan operations.
 * Uses Operand for type-safe operand representation supporting both literal values and parameter references.
 */
public record IndexScanPredicate(int id, String selector, Operator op, Operand operand) {

    /**
     * Tests if the given BqlValue satisfies this predicate.
     * For NE operator, performs the comparison; for other operators, returns true
     * (filtering is done at the index level).
     *
     * @param bqlValue   the value from the index to test
     * @param parameters the parameter list for resolving Param operands
     * @return true if the value satisfies the predicate
     */
    public boolean test(BqlValue bqlValue, List<BqlValue> parameters) {
        if (!(op.equals(Operator.NE))) {
            return true;
        }

        BqlValue resolvedOperand = operand.resolve(parameters);

        // NE
        return switch (resolvedOperand) {
            case StringVal(String expectedValue) -> {
                if (!(bqlValue instanceof StringVal(String actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case Int32Val(int expectedValue) -> {
                if (bqlValue instanceof Int32Val(int actualValue)) {
                    yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
                }
                yield numericNeCrossType(bqlValue, BsonType.INT32, expectedValue);
            }
            case Int64Val(long expectedValue) -> {
                if (bqlValue instanceof Int64Val(long actualValue)) {
                    yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
                }
                yield numericNeCrossType(bqlValue, BsonType.INT64, expectedValue);
            }
            case DoubleVal(double expectedValue) -> {
                if (bqlValue instanceof DoubleVal(double actualValue)) {
                    yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
                }
                yield numericNeCrossType(bqlValue, BsonType.DOUBLE, expectedValue);
            }
            case Decimal128Val decimal128Val -> {
                if (bqlValue instanceof Decimal128Val(BigDecimal value)) {
                    yield PredicateEvaluator.evaluateComparison(this.op(), value, decimal128Val.value());
                }
                yield numericNeCrossType(bqlValue, BsonType.DECIMAL128, decimal128Val.value());
            }
            case BooleanVal(boolean expectedValue) -> {
                if (!(bqlValue instanceof BooleanVal(boolean actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case NullVal ignored -> !(bqlValue instanceof NullVal);
            case BinaryVal(byte[] expectedValue) -> {
                if (!(bqlValue instanceof BinaryVal(byte[] actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case TimestampVal(long expectedValue) -> {
                if (!(bqlValue instanceof TimestampVal(long actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case DateTimeVal(long expectedValue) -> {
                if (!(bqlValue instanceof DateTimeVal(long actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case VersionstampVal versionstampVal -> {
                if (!(bqlValue instanceof VersionstampVal(Versionstamp value))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), value.getBytes(), versionstampVal.value().getBytes());
            }
            case ArrayVal arrayVal -> {
                if (!(bqlValue instanceof ArrayVal(List<BqlValue> values))) {
                    yield false;
                }
                yield !arrayVal.values().equals(values);
            }
            case DocumentVal documentVal -> {
                if (!(bqlValue instanceof DocumentVal(Map<String, BqlValue> fields))) {
                    yield false;
                }
                yield !documentVal.fields().equals(fields);
            }
            case ObjectIdVal(ObjectId expectedValue) -> {
                if (!(bqlValue instanceof ObjectIdVal(ObjectId actualValue))) {
                    yield false;
                }
                yield !expectedValue.equals(actualValue);
            }
            // $regex is never index-resolved: it always runs as a residual full-scan filter.
            case RegexVal ignored -> throw new UnsupportedOperationException("$regex cannot be evaluated on an index scan");
        };
    }

    @SuppressWarnings("unchecked")
    private boolean numericNeCrossType(BqlValue actualBqlValue, BsonType expectedType, Object expectedJavaValue) {
        BsonType actualType = switch (actualBqlValue) {
            case Int32Val ignored -> BsonType.INT32;
            case Int64Val ignored -> BsonType.INT64;
            case DoubleVal ignored -> BsonType.DOUBLE;
            case Decimal128Val ignored -> BsonType.DECIMAL128;
            default -> null;
        };
        if (actualType == null) {
            return false;
        }
        BsonType common = NumericWidening.commonType(actualType, expectedType);
        if (common == null) {
            return false;
        }
        Object actualJavaValue = switch (actualBqlValue) {
            case Int32Val(int v) -> v;
            case Int64Val(long v) -> v;
            case DoubleVal(double v) -> v;
            case Decimal128Val(BigDecimal v) -> v;
            default -> null;
        };
        Comparable<Object> promotedActual = (Comparable<Object>) NumericWidening.promoteToComparable(actualJavaValue, actualType, common);
        Comparable<Object> promotedExpected = (Comparable<Object>) NumericWidening.promoteToComparable(expectedJavaValue, expectedType, common);
        if (promotedActual == null || promotedExpected == null) {
            return false;
        }
        return PredicateEvaluator.evaluateComparison(this.op(), promotedActual, promotedExpected);
    }
}
