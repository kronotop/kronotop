package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

public class LogicalOrFilter extends LogicalFilter {

    public LogicalOrFilter() {
        super(Condition.OR);
    }

    @Override
    public String toString() {
        return "LogicalOrFilter [filters=" + filters + "]";
    }
}
