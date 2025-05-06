// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.operators.array;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.values.Int32Val;

public class BqlSizeOperator extends BqlOperator {
    public static final String NAME = "$SIZE";

    public BqlSizeOperator(int level) {
        super(level, OperatorType.SIZE);
    }

    @SuppressWarnings("unchecked")
    public int getSize() {
        Int32Val int32Val = (Int32Val) getValues().getFirst();
        return int32Val.value();
    }

    @Override
    public String toString() {
        return "BqlSizeOperator { level=" + getLevel() + ", values=" + getValues() + " }";
    }
}
