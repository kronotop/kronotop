package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlNeOperator extends BqlOperator {
    public static final String NAME = "$NE";

    public BqlNeOperator(int level) {
        super(level, OperatorType.NE);
    }

    @Override
    public String toString() {
        return "BqlNeOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}