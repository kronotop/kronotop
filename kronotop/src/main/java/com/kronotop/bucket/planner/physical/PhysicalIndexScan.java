// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.index.Index;

public class PhysicalIndexScan extends PhysicalScan {
    private final Index index;

    public PhysicalIndexScan(Index index, OperatorType operatorType) {
        super(operatorType);
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "PhysicalIndexScan {" +
                "index=" + index + ", " +
                "operatorType=" + getOperatorType() + ", " +
                "field=" + getField() + ", " +
                "value=" + bqlValue() +
                "bounds=" + getBounds() + "}";
    }
}
