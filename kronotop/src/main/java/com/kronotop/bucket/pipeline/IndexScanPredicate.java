package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.bql.ast.Int32Val;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.planner.Operator;

import java.util.Arrays;
import java.util.List;

public record IndexScanPredicate(int id, String selector, Operator op, Object operand) implements Predicate {
    @Override
    public boolean canEvaluate() {
        return op.equals(Operator.NE);
    }

    private List<Object> validateListOperand(Object expected, String operatorName) {
        if (expected instanceof List<?> expectedList) {
            return (List<Object>) expectedList;
        } else {
            throw new UnsupportedOperationException(operatorName + " operator requires a list of expected values");
        }
    }

    private <T> boolean evaluateComparison(Operator op, T actual, T expected) {
        return switch (op) {
            case EQ -> {
                if (actual instanceof byte[] actualBytes && expected instanceof byte[] expectedBytes) {
                    yield Arrays.equals(actualBytes, expectedBytes);
                } else {
                    yield actual.equals(expected);
                }
            }
            case NE -> {
                if (actual instanceof byte[] actualBytes && expected instanceof byte[] expectedBytes) {
                    yield !Arrays.equals(actualBytes, expectedBytes);
                } else {
                    yield !actual.equals(expected);
                }
            }
            case GT, GTE, LT, LTE -> {
                if (actual instanceof String actualStr && expected instanceof String expectedStr) {
                    int cmp = actualStr.compareTo(expectedStr);
                    yield switch (op) {
                        case GT -> cmp > 0;
                        case GTE -> cmp >= 0;
                        case LT -> cmp < 0;
                        case LTE -> cmp <= 0;
                        default -> false;
                    };
                } else if (actual instanceof byte[] actualBytes && expected instanceof byte[] expectedBytes) {
                    int cmp = Arrays.compare(actualBytes, expectedBytes);
                    yield switch (op) {
                        case GT -> cmp > 0;
                        case GTE -> cmp >= 0;
                        case LT -> cmp < 0;
                        case LTE -> cmp <= 0;
                        default -> false;
                    };
                } else {
                    throw new UnsupportedOperationException("Comparison operator " + op + " not supported for type: " + actual.getClass().getSimpleName());
                }
            }
            case IN -> {
                List<Object> expectedList = validateListOperand(expected, "IN");
                yield expectedList.contains(actual);
            }
            case NIN -> {
                List<Object> expectedList = validateListOperand(expected, "NIN");
                yield !expectedList.contains(actual);
            }
            default -> throw new UnsupportedOperationException("Comparison not supported for operator: " + op);
        };
    }

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
                yield evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case Int32Val(int expectedValue) -> {
                if (!(bqlValue instanceof Int32Val(int actualValue))) {
                    yield false;
                }
                yield evaluateComparison(this.op(), actualValue, expectedValue);
            }
            default -> throw new IllegalStateException("Unexpected value: " + this.operand());
        };
    }
}
