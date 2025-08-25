package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.bql.ast.Int32Val;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.planner.Operator;

public record IndexScanPredicate(int id, String selector, Operator op, Object operand) implements Predicate {
    @Override
    public boolean canEvaluate() {
        return op.equals(Operator.NE);
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
                yield FilterEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            case Int32Val(int expectedValue) -> {
                if (!(bqlValue instanceof Int32Val(int actualValue))) {
                    yield false;
                }
                yield FilterEvaluator.evaluateComparison(this.op(), actualValue, expectedValue);
            }
            default -> throw new IllegalStateException("Unexpected value: " + this.operand());
        };
    }
}
