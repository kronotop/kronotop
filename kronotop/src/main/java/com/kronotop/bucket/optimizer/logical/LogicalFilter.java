package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.OperationType;

public class LogicalFilter extends LogicalNode {
    private final OperationType operationType;

    public LogicalFilter(OperationType operationType) {
        this.operationType = operationType;
    }

    public OperationType getOperationType() {
        return operationType;
    }
}
