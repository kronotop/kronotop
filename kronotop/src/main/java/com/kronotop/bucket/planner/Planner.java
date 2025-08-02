/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket.planner;

import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PhysicalPlanner;

public class Planner {
    private final Context context;

    public Planner(Context context) {
        this.context = context;
    }

    public PhysicalNode plan(BucketMetadata metadata, String query) {
        LogicalPlanner logicalPlanner = new LogicalPlanner(query);
        LogicalNode logicalPlan = logicalPlanner.plan();
        PhysicalPlanner planner = new PhysicalPlanner(new PlannerContext(metadata), logicalPlan);
        return planner.plan();
    }
}
