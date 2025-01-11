package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlNinOperator extends BqlOperator {
    public static final String NAME = "$NIN";

    public BqlNinOperator(int level) {
        super(level, OperatorType.NIN);
    }

    @Override
    public String toString() {
        return "BqlNinOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
