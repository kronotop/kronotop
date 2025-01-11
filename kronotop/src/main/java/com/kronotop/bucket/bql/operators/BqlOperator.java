package com.kronotop.bucket.bql.operators;

import com.kronotop.bucket.bql.BqlValue;

import java.util.LinkedList;
import java.util.List;

public class BqlOperator {
    private final int level;
    private final OperatorType operatorType;
    private List<BqlValue<?>> values;

    public BqlOperator(int level, OperatorType operatorType) {
        this.level = level;
        this.operatorType = operatorType;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public int getLevel() {
        return level;
    }

    public void addValue(BqlValue<?> value) {
        if (values == null) {
            values = new LinkedList<>();
        }
        values.add(value);
    }

    public List<BqlValue<?>> getValues() {
        return values;
    }
}
