/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.pipeline;

import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Planner;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ParameterExtractor;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.planner.physical.PlannerContext;

import java.util.List;

/**
 * Builds a {@link MaterializedScanNode} from a {@link CandidateSupplier} and a BQL filter.
 * Converts the filter to a residual predicate and wraps the supplier into a scan node
 * compatible with the standard pipeline executor.
 */
public class MaterializedPlanBuilder {
    /**
     * Builds a materialized plan from a candidate supplier and a BQL filter.
     *
     * @param context          the application context
     * @param metadata         the bucket metadata for query planning
     * @param planner          the query planner
     * @param filterBytes      the BQL filter as raw bytes
     * @param supplier         the candidate supplier that delivers document locations on demand
     * @param planCacheEnabled whether to use the plan cache
     * @param planCacheMaxTtl  plan cache TTL in milliseconds
     * @return the materialized plan, or null if the filter is contradictory
     */
    public static MaterializedPlan build(
            Context context,
            BucketMetadata metadata,
            Planner planner,
            byte[] filterBytes,
            CandidateSupplier supplier,
            boolean planCacheEnabled,
            long planCacheMaxTtl) {

        // Step 1: Parse and plan the BQL filter through the normal pipeline
        BqlExpr expr = BqlParser.parse(filterBytes);
        List<BqlValue> parameters = ParameterExtractor.extract(expr);
        PipelineNode pipelineNode = planner.plan(context, metadata, expr, parameters, planCacheEnabled, planCacheMaxTtl);

        if (pipelineNode == null) {
            // Contradictory filter
            return null;
        }

        // Step 2: Convert the pipeline node to a residual predicate
        PlannerContext plannerCtx = new PlannerContext(metadata);
        ResidualPredicateNode predicate = PipelineRewriter.transformNodeToResidualPredicate(plannerCtx, pipelineNode);

        // Step 3: Create the MaterializedScanNode
        int nodeId = plannerCtx.nextId();
        MaterializedScanNode scanNode = new MaterializedScanNode(nodeId, supplier, predicate);
        return new MaterializedPlan(scanNode, parameters);
    }
}
