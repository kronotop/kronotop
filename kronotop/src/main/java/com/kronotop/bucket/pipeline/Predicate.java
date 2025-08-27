package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.planner.Operator;

public sealed interface Predicate permits IndexScanPredicate, FullScanPredicate {
    int id();
    String selector();
    Operator op();
    Object operand();
}