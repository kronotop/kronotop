package com.kronotop.bucket.executor;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.planner.Operator;

import java.nio.ByteBuffer;

public record Predicate(int id, String selector, Operator op, Object operand) {
    public boolean test(Versionstamp versionstamp, ByteBuffer buffer) {
        return false;
    }
}
