package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlGtOperator extends BqlOperator {
    public static final String NAME = "$GT";

    public BqlGtOperator(int level) {
        super(level, OperatorType.GT);
    }

    @Override
    public String toString() {
        return "BqlGtOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}