package com.kronotop.bucket.bql.operators.logical;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlOrOperator extends BqlOperator {
    public static final String NAME = "$OR";

    public BqlOrOperator(int level) {
        super(level, OperatorType.OR);
    }

    @Override
    public String toString() {
        return "BqlOrOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
