package com.kronotop.bucket.planner.physical;

import java.util.List;

public class PhysicalUnionOperator extends PhysicalNode {
    public PhysicalUnionOperator(List<PhysicalFilter> filters) {
        super(filters);
    }

    @Override
    public String toString() {
        return "PhysicalUnionOperator {filters=" + filters + "}";
    }
}