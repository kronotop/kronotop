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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.executor.PlanExecutor;
import com.kronotop.bucket.executor.PlanExecutorConfig;
import com.kronotop.bucket.optimizer.Optimizer;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PlannerContext;
import com.kronotop.bucket.planner.physical.PhysicalPlanner;

import java.nio.ByteBuffer;
import java.util.Map;

public class QueryExecutor {
    private static final LogicalPlanner logicalPlanner = new LogicalPlanner();
    private static final PhysicalPlanner physicalPlanner = new PhysicalPlanner();
    private static final Optimizer optimizer = new Optimizer();

    public static Map<Versionstamp, ByteBuffer> execute(Context context, Transaction tr, QueryExecutorConfig config) {
        BqlExpr parsedQuery = BqlParser.parse(config.getQuery());
        LogicalNode logicalPlan = logicalPlanner.planAndValidate(parsedQuery);
        PlannerContext plannerContext = new PlannerContext();
        PhysicalNode physicalPlan = physicalPlanner.plan(config.getMetadata(), logicalPlan, plannerContext);
        PhysicalNode optimizedPlan = optimizer.optimize(config.getMetadata(), physicalPlan, plannerContext);

        PlanExecutorConfig planExecutorConfig = new PlanExecutorConfig(config.getMetadata(), optimizedPlan, plannerContext);
        planExecutorConfig.setLimit(config.getLimit());
        planExecutorConfig.setReverse(config.isReverse());
        planExecutorConfig.setPinReadVersion(config.getPinReadVersion());
        planExecutorConfig.setReadVersion(config.getReadVersion());

        // Use the cursor from the QueryExecutorConfig to maintain state across BUCKET.ADVANCE calls
        planExecutorConfig.cursor().copyStatesFrom(config.getCursor());

        PlanExecutor executor = new PlanExecutor(context, planExecutorConfig);
        Map<Versionstamp, ByteBuffer> results = executor.execute(tr);

        // Copy updated cursor state back to the QueryExecutorConfig for future BUCKET.ADVANCE calls
        config.getCursor().copyStatesFrom(planExecutorConfig.cursor());

        return results;
    }
}
