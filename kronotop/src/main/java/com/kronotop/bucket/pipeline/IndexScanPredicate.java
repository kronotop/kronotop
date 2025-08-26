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
                if (!(bqlValue instanceof Decimal128Val actualDecimal128Val)) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualDecimal128Val.value(), decimal128Val.value());
            }
            case BooleanVal(boolean expectedValue) -> {
                if (!(bqlValue instanceof BooleanVal(boolean actualValue))) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case NullVal ignored -> {
                yield !(bqlValue instanceof NullVal);
            }
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
                if (!(bqlValue instanceof VersionstampVal actualVersionstampVal)) {
                    yield false;
                }
                yield PredicateEvaluator.evaluateComparison(this.op(), actualVersionstampVal.value().getBytes(), versionstampVal.value().getBytes());
            }
            case ArrayVal arrayVal -> {
                if (!(bqlValue instanceof ArrayVal actualArrayVal)) {
                    yield false;
                }
                yield !arrayVal.values().equals(actualArrayVal.values());
            }
            case DocumentVal documentVal -> {
                if (!(bqlValue instanceof DocumentVal actualDocumentVal)) {
                    yield false;
                }
                yield !documentVal.fields().equals(actualDocumentVal.fields());
            }
            default -> throw new IllegalStateException("Unexpected value: " + this.operand());
        };
    }
}
