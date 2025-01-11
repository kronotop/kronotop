package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlEqOperator extends BqlOperator {
    public static final String NAME = "$EQ";
    private String field;

    public BqlEqOperator(int level) {
        super(level, OperatorType.EQ);
    }

    public BqlEqOperator(int level, String field) {
        super(level, OperatorType.EQ);
        this.field = field;
    }

    public String getField() {
        return field;
    }

    @Override
    public String toString() {
        return "BqlEqOperator { level=" + getLevel() + ", field=" + field + ", values=" + getValues() + " }";
    }
}
