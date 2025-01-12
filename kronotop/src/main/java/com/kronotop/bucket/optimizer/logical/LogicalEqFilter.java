package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

public class LogicalEqFilter extends LogicalComparisonFilter {

    public LogicalEqFilter() {
        super(Condition.EQ);
    }

    @Override
    public String toString() {
        return "LogicalEqFilter [field=" + getField() + ", value=" + getValue() + "]";
    }
}
