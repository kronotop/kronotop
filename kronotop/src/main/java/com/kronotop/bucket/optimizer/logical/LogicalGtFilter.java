package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

public class LogicalGtFilter extends LogicalComparisonFilter {
    public LogicalGtFilter() {
        super(Condition.GT);
    }

    @Override
    public String toString() {
        return "LogicalGtFilter [field=" + getField() + ", value=" + getValue() + "]";
    }
}
