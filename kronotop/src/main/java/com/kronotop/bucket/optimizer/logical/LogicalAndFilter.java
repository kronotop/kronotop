package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.optimizer.Condition;

import java.util.LinkedList;
import java.util.List;

public class LogicalAndFilter extends LogicalFilter {
    private final List<LogicalFilter> filters = new LinkedList<>();

    public LogicalAndFilter() {
        super(Condition.AND);
    }

    public void addFilter(LogicalFilter filter) {
        filters.add(filter);
    }

    public List<LogicalFilter> getFilter() {
        return filters;
    }

    @Override
    public String toString() {
        return "LogicalAndFilter [filters=" + filters + "]";
    }
}
