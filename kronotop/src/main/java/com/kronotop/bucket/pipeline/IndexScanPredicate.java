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

import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.Operator;

public record IndexScanPredicate(int id, String selector, Operator op, Object operand) {
    public boolean test(BqlValue bqlValue) {
        if (!(op.equals(Operator.NE))) {
            return true;
        }
        // NE
        return switch (this.operand()) {
            case StringVal(String expectedValue) -> {
                if (!(bqlValue instanceof StringVal(String actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case Int32Val(int expectedValue) -> {
                if (!(bqlValue instanceof Int32Val(int actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case Int64Val(long expectedValue) -> {
                if (!(bqlValue instanceof Int64Val(long actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case DoubleVal(double expectedValue) -> {
                if (!(bqlValue instanceof DoubleVal(double actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case Decimal128Val decimal128Val -> {
                if (!(bqlValue instanceof Decimal128Val(java.math.BigDecimal value))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), value, decimal128Val.value());
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
                if (!(bqlValue instanceof VersionstampVal(com.apple.foundationdb.tuple.Versionstamp value))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), value.getBytes(), versionstampVal.value().getBytes());
            }
            case ArrayVal arrayVal -> {
                if (!(bqlValue instanceof ArrayVal(java.util.List<BqlValue> values))) {
                    yield false;
                }
                yield !arrayVal.values().equals(values);
            }
            case DocumentVal documentVal -> {
                if (!(bqlValue instanceof DocumentVal(java.util.Map<String, BqlValue> fields))) {
                    yield false;
                }
                yield !documentVal.fields().equals(fields);
            }
            default -> throw new IllegalStateException("Unexpected value: " + this.operand());
        };
    }
}
