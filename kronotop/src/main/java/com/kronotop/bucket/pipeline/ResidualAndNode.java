package com.kronotop.bucket.pipeline;

import java.nio.ByteBuffer;
import java.util.List;

public class ResidualAndNode implements ResidualPredicateNode {
    private final List<ResidualPredicateNode> children;

    public ResidualAndNode(List<ResidualPredicateNode> children) {
        this.children = children;
    }

    @Override
    public boolean test(ByteBuffer document) {
        boolean matched = true;
        for (ResidualPredicateNode predicate : children) {
            if (!predicate.test(document)) {
                matched = false;
                break;
            }
        }
        return matched;
    }
}
