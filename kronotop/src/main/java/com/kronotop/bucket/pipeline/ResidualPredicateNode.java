package com.kronotop.bucket.pipeline;

import java.nio.ByteBuffer;

public interface ResidualPredicateNode {
    boolean test(ByteBuffer document);
}
