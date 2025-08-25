package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.planner.Operator;

public record IndexScanPredicate(int id, String selector, Operator op, Object operand) implements Predicate {
    @Override
    public boolean canEvaluate() {
        // IndexScanPredicate cannot be applied to NE operator
        return !op.equals(Operator.NE);
    }

    public boolean test(BqlValue value) {
        // TODO: implement a filter for NE operator
        return true;
    }
}
