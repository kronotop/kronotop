package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.OperationType;

public class LogicalEqFilter extends LogicalComparisonFilter {

    public LogicalEqFilter() {
        super(OperationType.EQ);
    }

    @Override
    public String toString() {
        return "LogicalEqFilter [field=" + getField() + ", value=" + getValue() + "]";
    }
}
