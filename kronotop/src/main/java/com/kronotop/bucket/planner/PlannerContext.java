// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner;

import com.kronotop.bucket.index.Index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PlannerContext {
    private final Map<String, Index> indexes;

    public PlannerContext() {
        this.indexes = Collections.unmodifiableMap(new HashMap<>());
    }

    public PlannerContext(Map<String, Index> indexes) {
        this.indexes = Collections.unmodifiableMap(indexes);
    }

    public Map<String, Index> indexes() {
        return indexes;
    }
}
