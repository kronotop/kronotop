package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;

public class FullScanPredicate implements Predicate {
    public boolean test(Versionstamp versionstamp, ByteBuffer buffer) {
        return true;
    }

    @Override
    public boolean canEvaluate() {
        return false;
    }
}
