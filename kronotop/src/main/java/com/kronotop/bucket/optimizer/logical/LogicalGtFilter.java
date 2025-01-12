package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.OperationType;

public class LogicalGtFilter extends LogicalComparisonFilter {

    public LogicalGtFilter() {
        super(OperationType.GT);
    }

    @Override
    public String toString() {
        return "LogicalGtFilter [field=" + getField() + ", value=" + getValue() + "]";
    }
}
