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

package com.kronotop.bucket;

import com.kronotop.Context;
import com.kronotop.bucket.bql.QueryShape;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.optimizer.Optimizer;
import com.kronotop.bucket.pipeline.PhysicalPlanParameterBinder;
import com.kronotop.bucket.pipeline.PhysicalPlanParameterBinder.ParamBinding;
import com.kronotop.bucket.pipeline.PipelineContext;
import com.kronotop.bucket.pipeline.PipelineNode;
import com.kronotop.bucket.pipeline.PipelineRewriter;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PhysicalPlanValidator;
import com.kronotop.bucket.planner.physical.PhysicalPlanner;
import com.kronotop.bucket.planner.physical.PlannerContext;

import java.util.List;
import java.util.Map;

/**
 * Orchestrates the query planning pipeline for BQL queries with optional plan caching.
 *
 * <p>Transforms a parsed BQL expression into an executable pipeline through these stages:</p>
 * <ol>
 *   <li>Logical planning: AST → logical plan ({@link LogicalPlanner})</li>
 *   <li>Physical planning: logical plan → physical plan ({@link PhysicalPlanner})</li>
 *   <li>Optimization: applies index selection and plan transformations ({@link Optimizer})</li>
 *   <li>Parameter binding: maps physical nodes to parameter indices ({@link PhysicalPlanParameterBinder})</li>
 *   <li>Pipeline rewriting: physical plan → executable pipeline ({@link PipelineRewriter})</li>
 * </ol>
 *
 * <p>Plan caching uses {@link QueryShape} to compute a structural hash of queries.
 * Queries with identical shapes (same operators, fields, and value types) share cached plans,
 * avoiding redundant planning overhead. Cached plans are parameterized: the pipeline structure
 * is reused while actual parameter values are bound at execution time via {@link PipelineContext}.</p>
 *
 * @see LogicalPlanner
 * @see PhysicalPlanner
 * @see Optimizer
 * @see PhysicalPlanParameterBinder
 * @see PipelineRewriter
 * @see PlanCache
 * @see QueryShape
 */
public class Planner {
    private final LogicalPlanner logicalPlanner = new LogicalPlanner();
    private final PhysicalPlanner physicalPlanner = new PhysicalPlanner();
    private final Optimizer optimizer = new Optimizer();
    private final PlanCache planCache;

    /**
     * Creates a new Planner with the specified plan cache.
     *
     * @param planCache the cache for storing and retrieving compiled plans
     */
    public Planner(PlanCache planCache) {
        this.planCache = planCache;
    }

    /**
     * Plans a BQL expression into an executable pipeline with optional caching.
     *
     * @param context      the context providing the current time for TTL checks
     * @param metadata     the bucket metadata containing index definitions
     * @param bqlExpr      the parsed BQL expression (AST)
     * @param parameters   the extracted parameter values for binding to the plan
     * @param usePlanCache if true, attempts to retrieve a cached plan or caches the newly created plan
     * @param maxTTL       maximum time-to-live in milliseconds for cached plans
     * @return the executable pipeline node
     */
    public PipelineNode plan(
            Context context,
            BucketMetadata metadata,
            BqlExpr bqlExpr,
            List<BqlValue> parameters,
            boolean usePlanCache,
            long maxTTL
    ) {
        return plan(context, metadata, bqlExpr, parameters, usePlanCache, maxTTL, null);
    }

    public PipelineNode plan(
            Context context,
            BucketMetadata metadata,
            BqlExpr bqlExpr,
            List<BqlValue> parameters,
            boolean usePlanCache,
            long maxTTL,
            String sortByField
    ) {
        return plan(context, metadata, bqlExpr, parameters, usePlanCache, maxTTL, sortByField, null);
    }

    /**
     * Plans a BQL expression into an executable pipeline with SORTBY-aware index selection and per-query collation.
     *
     * @param context      the context providing the current time for TTL checks
     * @param metadata     the bucket metadata containing index definitions
     * @param bqlExpr      the parsed BQL expression (AST)
     * @param parameters   the extracted parameter values for binding to the plan
     * @param usePlanCache if true, attempts to retrieve a cached plan or caches the newly created plan
     * @param maxTTL       maximum time-to-live in milliseconds for cached plans
     * @param sortByField  the field to sort by, used to prefer matching indexes (maybe null)
     * @param collation    the per-query collation override (maybe null)
     * @return the executable pipeline node
     */
    public PipelineNode plan(
            Context context,
            BucketMetadata metadata,
            BqlExpr bqlExpr,
            List<BqlValue> parameters,
            boolean usePlanCache,
            long maxTTL,
            String sortByField,
            Collation collation
    ) {
        long shapeHash = QueryShape.compute(bqlExpr, sortByField, collation);
        if (usePlanCache) {
            PlanCache.CachedPlan cachedPlan = planCache.get(metadata.namespace(), metadata.uuid(), shapeHash);
            if (cachedPlan != null && !(context.now() >= cachedPlan.insertedAt() + maxTTL)) {
                return cachedPlan.plan();
            }
        }

        PlannerContext ctx = new PlannerContext(metadata);
        if (sortByField != null) {
            ctx.setSortByField(sortByField);
        }
        if (collation != null) {
            ctx.setCollation(collation);
        }
        LogicalNode logicalPlan = logicalPlanner.planAndValidate(bqlExpr);
        PhysicalNode physicalPlan = physicalPlanner.plan(ctx, logicalPlan);
        PhysicalNode optimizedPlan = optimizer.optimize(ctx, physicalPlan);
        PhysicalPlanValidator.validate(ctx, optimizedPlan);

        // Create a parameterized pipeline context with bindings
        Map<Integer, List<ParamBinding>> bindings = PhysicalPlanParameterBinder.bind(optimizedPlan, parameters);
        PipelineContext pipelineCtx = new PipelineContext(bindings, parameters);
        PipelineNode plan = PipelineRewriter.rewrite(ctx, pipelineCtx, optimizedPlan);
        if (usePlanCache) {
            planCache.put(metadata.namespace(), metadata.uuid(), shapeHash, plan);
        }
        return plan;
    }
}
