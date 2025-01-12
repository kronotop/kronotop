package com.kronotop.bucket.bql.operators.comparison;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlInOperator extends BqlOperator {
    public static final String NAME = "$IN";

    public BqlInOperator(int level) {
        super(level, OperatorType.IN);
    }

    @Override
    public String toString() {
        return "BqlInOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
