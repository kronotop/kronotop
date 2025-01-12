package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.bql.operators.OperatorType;

public class LogicalFilter extends LogicalNode {
    private final OperatorType operatorType;

    public LogicalFilter(OperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }
}
