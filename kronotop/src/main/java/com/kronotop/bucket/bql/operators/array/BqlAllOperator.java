package com.kronotop.bucket.bql.operators.array;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;

public class BqlAllOperator extends BqlOperator {
    public static final String NAME = "$ALL";

    public BqlAllOperator(int level) {
        super(level, OperatorType.ALL);
    }

    @Override
    public String toString() {
        return "BqlAllOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
