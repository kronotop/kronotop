package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;

public class BqlGtOperator extends BqlOperator {
    public static final String NAME = "$GT";

    public BqlGtOperator(int level) {
        super(level);
    }

    @Override
    public String toString() {
        return "BqlGtOperator { level=" + level + ", values=" + getValues() + " }";
    }
}