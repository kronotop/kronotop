package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

public class LogicalAndFilter extends LogicalFilter {

    public LogicalAndFilter() {
        super(Condition.AND);
    }

    @Override
    public String toString() {
        return "LogicalAndFilter [filters=" + filters + "]";
    }
}
