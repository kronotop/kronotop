package com.kronotop.bucket.bql.operators.logical;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlAndOperator extends BqlOperator {
    public static final String NAME = "$AND";

    public BqlAndOperator(int level) {
        super(level, OperatorType.AND);
    }

    @Override
    public String toString() {
        return "BqlAndOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
