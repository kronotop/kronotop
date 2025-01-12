package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.OperationType;

public class LogicalOrFilter extends LogicalFilter {

    public LogicalOrFilter() {
        super(OperationType.OR);
    }

    @Override
    public String toString() {
        return "LogicalOrFilter [filters=" + filters + "]";
    }
}
