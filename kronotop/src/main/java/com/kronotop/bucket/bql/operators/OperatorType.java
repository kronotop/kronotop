// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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
    NOR(12),
    SIZE(13),
    ELEM_MATCH(14);

    private final int value;

    OperatorType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
