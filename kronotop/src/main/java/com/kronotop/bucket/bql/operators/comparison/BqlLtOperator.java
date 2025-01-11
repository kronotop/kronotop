package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlLtOperator extends BqlOperator {
    public static final String NAME = "$LT";

    public BqlLtOperator(int level) {
        super(level, OperatorType.LT);
    }

    @Override
    public String toString() {
        return "BqlLtOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
