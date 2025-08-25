package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;

public interface Predicate {
    boolean isApplicable();
    boolean test(Versionstamp versionstamp, ByteBuffer buffer);
}
