package com.kronotop.bucket.bql.operators.array;

import com.kronotop.bucket.bql.operators.BqlOperator;

public class BqlAllOperator extends BqlOperator {
    public static final String NAME = "$ALL";

    public BqlAllOperator(int level) {
        super(level);
    }

    @Override
    public String toString() {
        return "BqlAllOperator { level=" + level + ", values=" + getValues() + " }";
    }
}
