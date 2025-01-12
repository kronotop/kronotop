package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.bql.operators.OperatorType;

public class LogicalComparisonFilter extends LogicalFilter {
    private BqlValue<?> value;
    private String field;

    public LogicalComparisonFilter(OperatorType operatorType) {
        super(operatorType);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void addValue(BqlValue<?> value) {
        this.value = value;
    }

    public BqlValue<?> getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "LogicalComparisonFilter [" +
                "operatorType=" + getOperatorType() + ", " +
                "field=" + getField() + ", " +
                "value=" + getValue() + "]";
    }
}
