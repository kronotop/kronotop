/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.sql.executor;

import com.kronotop.core.Context;
import com.kronotop.core.TransactionUtils;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.PrepareResponse;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.optimizer.physical.*;
import org.apache.calcite.rel.RelNode;

import javax.annotation.Nonnull;

/**
 * The PlanExecutor class is responsible for executing a given executor by visiting its relational nodes.
 */
public class PlanExecutor extends Visitors {

    public PlanExecutor(Context context) {
        super(context);
    }

    /**
     * Visits a given RelNode and performs corresponding operations based on its type.
     *
     * @param planContext The executor context for executing the executor.
     * @param relNode     The relational node to visit.
     * @throws SqlExecutionException if an error occurs during the execution of the executor.
     * @throws UnknownRelNodeException if the type of the relational node is unknown.
     */
    private void visit(PlanContext planContext, @Nonnull RelNode relNode) throws SqlExecutionException {
        switch (relNode) {
            case PhysicalValues node -> physicalValuesVisitor.visit(planContext, node);
            case PhysicalTableModify node -> physicalTableModifyVisitor.visit(planContext, node);
            case PhysicalProject node -> physicalProjectVisitor.visit(planContext, node);
            case PhysicalTableScan node -> physicalTableScanVisitor.visit(planContext, node);
            case PhysicalCalc node -> physicalCalcVisitor.visit(planContext, node);
            default -> throw new UnknownRelNodeException(relNode.getRelTypeName());
        }
    }

    /**
     * Recursively visits the relational nodes in a executor and performs corresponding operations based on their type.
     *
     * @param planContext The executor context for executing the executor.
     * @param relNode     The relational node to visit.
     * @throws SqlExecutionException   if an error occurs during the execution of the executor.
     * @throws UnknownRelNodeException if the type of the relational node is unknown.
     */
    private void walk(PlanContext planContext, RelNode relNode) throws SqlExecutionException {
        if (relNode.getInputs().isEmpty()) {
            visit(planContext, relNode);
        } else {
            for (RelNode item : relNode.getInputs()) {
                walk(planContext, item);
            }
            visit(planContext, relNode);
        }
    }

    /**
     * Executes a given executor by visiting its relational nodes.
     *
     * @param planContext The executor context for executing the executor.
     * @param plan        The executor to execute.
     * @return The executed RedisMessage.
     */
    public RedisMessage execute(PlanContext planContext, Plan plan) {
        // Set RelOptTable first for further use.
        planContext.setTable(plan.getQueryOptimizationResult().getRel().getTable());

        try {
            walk(planContext, plan.getQueryOptimizationResult().getRel());
        } catch (SqlExecutionException e) {
            return new ErrorRedisMessage(e.getMessage());
        }

        // Commit the changes if the "auto commit" is enabled.
        TransactionUtils.commitIfAutoCommitEnabled(planContext.getTransaction(), planContext.getChannelContext());

        // Prepare a response from the resulting PlanContext instance.
        return PrepareResponse.prepare(planContext);
    }
}
