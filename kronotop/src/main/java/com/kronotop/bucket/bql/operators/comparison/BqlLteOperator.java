package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlLteOperator extends BqlOperator {
    public static final String NAME = "$LTE";

    public BqlLteOperator(int level) {
        super(level, OperatorType.LTE);
    }

    @Override
    public String toString() {
        return "BqlLteOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
