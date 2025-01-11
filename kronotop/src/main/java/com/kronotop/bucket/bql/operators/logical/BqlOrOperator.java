package com.kronotop.bucket.bql.operators.logical;

import com.kronotop.bucket.bql.operators.BqlOperator;

public class BqlOrOperator extends BqlOperator {
    public static final String NAME = "$OR";

    public BqlOrOperator(int level) {
        super(level);
    }

    @Override
    public String toString() {
        return "BqlOrOperator { level=" + level + ", values=" + getValues() + " }";
    }
}
