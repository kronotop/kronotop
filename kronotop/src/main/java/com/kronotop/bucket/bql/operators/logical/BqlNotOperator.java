package com.kronotop.bucket.bql.operators.logical;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlNotOperator extends BqlOperator {
    public static final String NAME = "$NOT";

    public BqlNotOperator(int level) {
        super(level, OperatorType.NOT);
    }

    @Override
    public String toString() {
        return "BqlNotOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
