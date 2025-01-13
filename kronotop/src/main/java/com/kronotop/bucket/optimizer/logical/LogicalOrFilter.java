// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.bql.operators.OperatorType;

public class LogicalOrFilter extends LogicalFilter {

    public LogicalOrFilter() {
        super(OperatorType.OR);
    }

    @Override
    public String toString() {
        return "LogicalOrFilter [filters=" + filters + "]";
    }
}
