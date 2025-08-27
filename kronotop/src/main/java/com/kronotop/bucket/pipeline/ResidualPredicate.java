package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.planner.Operator;

import java.nio.ByteBuffer;

public record ResidualPredicate(int id, String selector, Operator op, Object operand) implements Predicate {
    public boolean test(Versionstamp versionstamp, ByteBuffer document) {
        try {
            return PredicateEvaluator.testResidualPredicate(this, document);
        } finally {
            document.rewind();
        }
    }
}
