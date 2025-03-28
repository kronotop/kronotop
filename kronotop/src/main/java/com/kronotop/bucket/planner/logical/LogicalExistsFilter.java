package com.kronotop.bucket.planner.logical;

import com.kronotop.bucket.bql.operators.OperatorType;

public class LogicalExistsFilter extends LogicalFilter {
    private String field;

    public LogicalExistsFilter() {
        super(OperatorType.EXISTS);
    }

    public String getField() {
        return field;
    }

    void setField(String field) {
        this.field = field;
    }

    @Override
    public String toString() {
        return "LogicalExistsFilter {" +
                "operatorType=" + getOperatorType() + ", " +
                "field=" + getField() + "}";
    }
}
