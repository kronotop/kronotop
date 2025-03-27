package com.kronotop.bucket.optimizer.physical;

import com.kronotop.bucket.bql.operators.OperatorType;

public class PhysicalFilter extends PhysicalNode {
    private final OperatorType operatorType;

    public PhysicalFilter(OperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }
}
