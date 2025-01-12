package com.kronotop.bucket.bql.operators;

public enum OperatorType {
    EQ(0),
    OR(1),
    LT(2),
    GT(3),
    ALL(4),
    NIN(5),
    AND(6),
    NOT(7),
    GTE(8),
    LTE(9),
    NE(10),
    IN(11),
    NOR(12);

    private final int value;

    OperatorType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
