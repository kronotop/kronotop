package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.OperationType;

public class LogicalAndFilter extends LogicalFilter {

    public LogicalAndFilter() {
        super(OperationType.AND);
    }

    @Override
    public String toString() {
        return "LogicalAndFilter [filters=" + filters + "]";
    }
}
