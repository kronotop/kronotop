package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

import java.util.LinkedList;
import java.util.List;

public class LogicalFilter extends LogicalNode {
    private final Condition condition;

    public LogicalFilter(Condition condition) {
        this.condition = condition;
    }

    public Condition getCondition() {
        return condition;
    }
}
