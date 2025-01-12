package com.kronotop.bucket.bql.operators.logical;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlNorOperator extends BqlOperator {
    public static final String NAME = "$NOR";

    public BqlNorOperator(int level) {
        super(level, OperatorType.NOR);
    }

    @Override
    public String toString() {
        return "BqlNorOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
