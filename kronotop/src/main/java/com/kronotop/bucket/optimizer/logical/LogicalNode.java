package com.kronotop.bucket.optimizer.logical;

import java.util.LinkedList;
import java.util.List;

public class LogicalNode {
    protected final List<LogicalFilter> filters = new LinkedList<>();

    public void addFilter(LogicalFilter filter) {
        filters.add(filter);
    }

    public void setFilters(List<LogicalFilter> filters) {
        this.filters.clear();
        this.filters.addAll(filters);
    }

    public List<LogicalFilter> getFilters() {
        return filters;
    }

    @Override
    public String toString() {
        return "LogicalNode [filters=" + filters + "]";
    }
}
