package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.bql.operators.OperatorType;

public class LogicalAndFilter extends LogicalFilter {

    public LogicalAndFilter() {
        super(OperatorType.AND);
    }

    @Override
    public String toString() {
        return "LogicalAndFilter [filters=" + filters + "]";
    }
}
