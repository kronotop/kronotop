package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.bql.operators.OperatorType;

import java.util.List;

public class PhysicalUnionOperator extends PhysicalFilter {
    public PhysicalUnionOperator(List<PhysicalNode> children) {
        super(OperatorType.OR, children);
    }

    @Override
    public String toString() {
        return "PhysicalUnionOperator {children=" + children + "}";
    }
}