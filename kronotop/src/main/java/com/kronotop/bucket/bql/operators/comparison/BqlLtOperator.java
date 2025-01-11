package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;

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
