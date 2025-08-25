package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.planner.Operator;

import java.nio.ByteBuffer;

public record IndexScanPredicate(int id, String selector, Operator op, Object operand) implements Predicate {
    @Override
    public boolean isApplicable() {
        // IndexScanPredicate cannot be applied to NE operator
        return !op.equals(Operator.NE);
    }

    @Override
    public boolean test(Versionstamp versionstamp, ByteBuffer buffer) {
        return false;
    }
}
