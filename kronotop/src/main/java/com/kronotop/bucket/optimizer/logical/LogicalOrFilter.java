package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.bql.operators.OperatorType;

public class LogicalOrFilter extends LogicalFilter {

    public LogicalOrFilter() {
        super(OperatorType.OR);
    }

    @Override
    public String toString() {
        return "LogicalOrFilter [filters=" + filters + "]";
    }
}
