// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner;

import com.kronotop.Context;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PhysicalPlanner;

import java.util.Map;

public class Planner {
    private final Context context;

    public Planner(Context context) {
        this.context = context;
    }

    public PhysicalNode plan(Map<String, Index> indexes, String query) {
        LogicalPlanner logicalPlanner = new LogicalPlanner(query);
        LogicalNode logicalPlan = logicalPlanner.plan();
        PhysicalPlanner planner = new PhysicalPlanner(new PlannerContext(indexes), logicalPlan);
        return planner.plan();
    }
}
