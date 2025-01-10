package com.kronotop.bql.operators.comparison;

import com.kronotop.bql.operators.BqlOperator;

public class BqlLtOperator extends BqlOperator {
    public static final String NAME = "$LT";

    public BqlLtOperator(int level) {
        super(level);
    }

    @Override
    public String toString() {
        return "BqlLtOperator { level=" + level + ", values=" + getValues() + " }";
    }
}
