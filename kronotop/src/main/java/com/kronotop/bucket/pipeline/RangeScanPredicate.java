package com.kronotop.bucket.pipeline;

public record RangeScanPredicate(String selector, Object lowerBound, Object upperBound, boolean includeLower,
                                 boolean includeUpper) {
}
