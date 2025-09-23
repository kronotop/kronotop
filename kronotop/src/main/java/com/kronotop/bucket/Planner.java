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

package com.kronotop.bucket;

import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.optimizer.Optimizer;
import com.kronotop.bucket.pipeline.PipelineNode;
import com.kronotop.bucket.pipeline.PipelineRewriter;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PhysicalPlanner;
import com.kronotop.bucket.planner.physical.PlannerContext;

public class Planner {
    private final LogicalPlanner logicalPlanner = new LogicalPlanner();
    private final PhysicalPlanner physicalPlanner = new PhysicalPlanner();
    private final Optimizer optimizer = new Optimizer();
    private final PlannerContext plannerCtx = new PlannerContext();

    public PipelineNode plan(BucketMetadata metadata, byte[] query) {
        BqlExpr parsedQuery = BqlParser.parse(query);
        LogicalNode logicalPlan = logicalPlanner.planAndValidate(parsedQuery);
        PhysicalNode physicalPlan = physicalPlanner.plan(metadata, logicalPlan, plannerCtx);
        PhysicalNode optimizedPlan = optimizer.optimize(metadata, physicalPlan, plannerCtx);
        return PipelineRewriter.rewrite(plannerCtx, optimizedPlan);
    }
}
