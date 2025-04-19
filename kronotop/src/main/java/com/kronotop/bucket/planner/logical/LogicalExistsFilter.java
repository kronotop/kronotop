package com.kronotop.bucket.planner.logical;

import com.kronotop.bucket.bql.operators.OperatorType;

public class LogicalExistsFilter extends LogicalFilter {
    private final boolean value;
    private String field;

    public LogicalExistsFilter(boolean value) {
        super(OperatorType.EXISTS);
        this.value = value;
    }

    public boolean getValue() {
        return value;
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
                "value=" + getValue() + ", " +
                "field=" + getField() + "}";
    }
}
