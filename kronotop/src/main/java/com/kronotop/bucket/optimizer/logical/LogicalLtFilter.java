package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.optimizer.Condition;

public class LogicalLtFilter extends LogicalFilter {
    private BqlValue<?> value;
    private String field;

    public LogicalLtFilter() {
        super(Condition.LT);
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
        return "LogicalLtFilter [field=" + field + ", value=" + value + "]";
    }
}
