package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.planner.Operator;

import java.nio.ByteBuffer;

public record FullScanPredicate(int id, String selector, Operator op, Object operand) implements Predicate {
    public boolean test(Versionstamp versionstamp, ByteBuffer buffer) {
        return true;
    }

    @Override
    public boolean canEvaluate() {
        return true;
    }
}
