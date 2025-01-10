package com.kronotop.bql.operators.comparison;

import com.kronotop.bql.operators.BqlOperator;

public class BqlEqOperator extends BqlOperator {
    public static final String NAME = "$EQ";
    private String field;

    public BqlEqOperator(int level) {
        super(level);
    }

    public BqlEqOperator(int level, String field) {
        super(level);
        this.field = field;
    }

    public String getField() {
        return field;
    }

    @Override
    public String toString() {
        return "BqlEqOperator { level=" + level + ", field=" + field + ", values=" + getValues() + "}";
    }
}
