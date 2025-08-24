package com.kronotop.bucket.executor;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;

public interface PredicateNode {
    boolean test(Versionstamp versionstamp, ByteBuffer buffer);
}
