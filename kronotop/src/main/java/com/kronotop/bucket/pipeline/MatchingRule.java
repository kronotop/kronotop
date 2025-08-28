package com.kronotop.bucket.pipeline;

public enum MatchingRule {
    ALL,  // All conditions must be met (AND)
    ANY,  // At least one condition is enough (OR)
    NONE  // No conditions should be met (NOT)
}