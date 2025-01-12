package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

import java.util.List;

public class LogicalAndFilter extends LogicalFilter {

    public LogicalAndFilter() {
        super(Condition.AND);
    }

    public LogicalAndFilter(List<LogicalFilter> filters) {
        super(Condition.AND);
        this.filters.addAll(filters);
    }

    @Override
    public String toString() {
        return "LogicalAndFilter [filters=" + filters + "]";
    }
}
