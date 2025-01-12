package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.optimizer.OperationType;

public class LogicalComparisonFilter extends LogicalFilter {
    private BqlValue<?> value;
    private String field;

    public LogicalComparisonFilter(OperationType operationType) {
        super(operationType);
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    public void addValue(BqlValue<?> value) {
        this.value = value;
    }

    public BqlValue<?> getValue() {
        return value;
    }
}
