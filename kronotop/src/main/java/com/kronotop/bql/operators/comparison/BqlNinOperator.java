package com.kronotop.bql.operators.comparison;

import com.kronotop.bql.operators.BqlOperator;

public class BqlNinOperator extends BqlOperator {
    public static final String NAME = "$NIN";

    public BqlNinOperator(int level) {
        super(level);
    }

    @Override
    public String toString() {
        return "BqlNinOperator { level=" + level + ", values=" + getValues() + "}";
    }
}
