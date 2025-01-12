package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlGteOperator extends BqlOperator {
    public static final String NAME = "$GTE";

    public BqlGteOperator(int level) {
        super(level, OperatorType.GTE);
    }

    @Override
    public String toString() {
        return "BqlGteOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
