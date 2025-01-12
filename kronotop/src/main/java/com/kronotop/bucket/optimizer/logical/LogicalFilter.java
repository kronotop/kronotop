package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

public class LogicalFilter {
    private final Condition condition;

    public LogicalFilter(Condition condition) {
        this.condition = condition;
    }

    public Condition getCondition() {
        return condition;
    }
}
