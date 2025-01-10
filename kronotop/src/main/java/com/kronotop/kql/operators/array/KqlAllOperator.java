package com.kronotop.kql.operators.array;

import com.kronotop.kql.KqlValue;
import com.kronotop.kql.operators.KqlBaseOperator;
import com.kronotop.kql.operators.KqlOperator;

public class KqlAllOperator extends KqlBaseOperator implements KqlOperator {
    public static final String NAME = "$ALL";
    private KqlValue<?> value;

    public KqlAllOperator(int level) {
        super(level);
    }

    @Override
    public KqlValue<?> getValue() {
        return value;
    }

    @Override
    public void setValue(KqlValue<?> value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return stringify(NAME, value);
    }
}
