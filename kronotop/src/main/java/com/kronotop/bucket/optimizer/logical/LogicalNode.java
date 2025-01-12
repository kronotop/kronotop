package com.kronotop.bucket.optimizer.logical;

import java.util.LinkedList;
import java.util.List;

public class LogicalNode {
    protected final List<LogicalFilter> filters = new LinkedList<>();

    public void addFilter(LogicalFilter filter) {
        filters.add(filter);
    }

    public List<LogicalFilter> getFilters() {
        return filters;
    }
}
