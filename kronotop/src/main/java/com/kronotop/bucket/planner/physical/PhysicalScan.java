package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.bql.operators.OperatorType;

public class PhysicalScan extends PhysicalFilter {
    private BqlValue<?> value;
    private String field;

    public PhysicalScan(OperatorType operatorType) {
        super(operatorType);
    }

    public String getField() {
        return field;
    }

    void setField(String field) {
        this.field = field;
    }

    void addValue(BqlValue<?> value) {
        this.value = value;
    }

    public BqlValue<?> getValue() {
        return value;
    }
}
