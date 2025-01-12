package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

import java.util.List;

public class LogicalOrFilter extends LogicalFilter {

    public LogicalOrFilter() {
        super(Condition.OR);
    }

    public LogicalOrFilter(List<LogicalFilter> filters) {
        super(Condition.OR);
        this.filters.addAll(filters);
    }

    @Override
    public String toString() {
        return "LogicalOrFilter [filters=" + filters + "]";
    }
}
