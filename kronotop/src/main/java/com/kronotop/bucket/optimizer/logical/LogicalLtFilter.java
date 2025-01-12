package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

public class LogicalLtFilter extends LogicalComparisonFilter {

    public LogicalLtFilter() {
        super(Condition.LT);
    }

    @Override
    public String toString() {
        return "LogicalLtFilter [field=" + getField() + ", value=" + getValue() + "]";
    }
}
