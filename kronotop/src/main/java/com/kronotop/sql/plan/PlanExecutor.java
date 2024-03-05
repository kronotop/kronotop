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

package com.kronotop.sql.plan;

import com.kronotop.core.Context;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.sql.Plan;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.optimizer.enumerable.EnumerableProject;
import com.kronotop.sql.optimizer.enumerable.EnumerableTableModify;
import com.kronotop.sql.optimizer.enumerable.EnumerableValues;
import org.apache.calcite.rel.RelNode;

import javax.annotation.Nonnull;

public class PlanExecutor extends Visitors {

    public PlanExecutor(Context context) {
        super(context);
    }

    private void visit(PlanContext planContext, @Nonnull RelNode relNode) {
        try {
            switch (relNode) {
                case EnumerableValues node -> enumerableValuesVisitor.visit(planContext, node);
                case EnumerableTableModify node -> enumerableTableModifyVisitor.visit(planContext, node);
                case EnumerableProject node -> enumerableProjectVisitor.visit(planContext, node);
                default -> throw new UnknownRelNodeException(relNode.getRelTypeName());
            }
        } catch (SqlExecutionException e) {
            planContext.setResponse(new ErrorRedisMessage(e.getMessage()));
        }
    }

    private void walk(PlanContext planContext, RelNode relNode) {
        if (relNode.getInputs().isEmpty()) {
            visit(planContext, relNode);
        } else {
            for (RelNode item : relNode.getInputs()) {
                walk(planContext, item);
            }
            visit(planContext, relNode);
        }
    }

    public void execute(PlanContext planContext, Plan plan) {
        walk(planContext, plan.getRelNode());
    }
}
