package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.OperationType;

public class LogicalLtFilter extends LogicalComparisonFilter {

    public LogicalLtFilter() {
        super(OperationType.LT);
    }

    @Override
    public String toString() {
        return "LogicalLtFilter [field=" + getField() + ", value=" + getValue() + "]";
    }
}
