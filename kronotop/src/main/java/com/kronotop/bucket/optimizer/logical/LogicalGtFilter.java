package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.optimizer.Condition;

public class LogicalGtFilter extends LogicalFilter {
    private BqlValue<?> value;
    private String field;

    public LogicalGtFilter() {
        super(Condition.GT);
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

    @Override
    public String toString() {
        return "LogicalGtFilter [field=" + field + ", value=" + value + "]";
    }
}
