package com.kronotop.bucket.optimizer.physical;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.bql.operators.OperatorType;

public class IndexComparisonFilter extends PhysicalFilter {
    private final String index;
    private BqlValue<?> value;
    private String bucket;
    private String field;

    public IndexComparisonFilter(String bucket, String index, OperatorType operatorType) {
        super(operatorType);
        this.bucket = bucket;
        this.index = index;
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

    public String getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "IndexComparisonFilter [" +
                "index=" + index + ", " +
                "operatorType=" + getOperatorType() + ", " +
                "field=" + getField() + ", " +
                "value=" + getValue() + "]";
    }
}
