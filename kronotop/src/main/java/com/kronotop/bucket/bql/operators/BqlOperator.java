// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.operators;

import com.kronotop.bucket.bql.BqlValue;

import java.util.LinkedList;
import java.util.List;

public class BqlOperator {
    private final int level;
    private final OperatorType operatorType;
    private List<BqlValue<?>> values;

    public BqlOperator(int level, OperatorType operatorType) {
        this.level = level;
        this.operatorType = operatorType;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public int getLevel() {
        return level;
    }

    public void addValue(BqlValue<?> value) {
        if (values == null) {
            values = new LinkedList<>();
        }
        values.add(value);
    }

    public List<BqlValue<?>> getValues() {
        return values;
    }
}
