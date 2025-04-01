// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.executor;

import com.kronotop.Context;
import com.kronotop.bucket.planner.physical.PhysicalNode;

import java.util.List;

public class PlanExecutor {
    private final Context context;
    private final PhysicalNode plan;

    public PlanExecutor(Context context, PhysicalNode plan) {
        this.context = context;
        this.plan = plan;
    }

    public List<byte[]> execute() {
        return List.of();
    }
}
