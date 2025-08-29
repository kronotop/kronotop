package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.NullVal;
import com.kronotop.bucket.planner.Operator;

import java.nio.ByteBuffer;

public record ResidualPredicate(int id, String selector, Operator op, Object operand) implements ResidualPredicateNode {
    public boolean test(ByteBuffer document) {
        switch (op) {
            case LT, LTE, GT, GTE:
                if (operand instanceof NullVal) {
                    return false;
                }
        }

        try {
            return PredicateEvaluator.testResidualPredicate(this, document);
        } finally {
            document.rewind();
        }
    }
}
