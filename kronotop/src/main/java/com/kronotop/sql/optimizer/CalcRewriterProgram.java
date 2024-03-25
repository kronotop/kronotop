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

package com.kronotop.sql.optimizer;

import com.kronotop.sql.optimizer.logical.LogicalRules;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.*;

public class CalcRewriterProgram {
    private static final HepProgram HEP_CALC_REWRITER_PROGRAM;

    static {
        HEP_CALC_REWRITER_PROGRAM = prepareCalcRewriterProgram();
    }

    // Note: it must be used only once in static class initializer.
    private static HepProgram prepareCalcRewriterProgram() {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

        // Filter rules
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_MERGE)
                .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
                .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
                .addRuleInstance(PruneEmptyRules.FILTER_INSTANCE);

        // Project rules
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_MERGE)
                .addRuleInstance(CoreRules.PROJECT_REMOVE)
                .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);

        // Calc rules
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC)
                .addRuleInstance(CoreRules.FILTER_TO_CALC)
                .addRuleInstance(CalcMergeRule.Config.DEFAULT.toRule())
                .addRuleInstance(CoreRules.CALC_REMOVE)
                .addRuleInstance(FilterToCalcRule.Config.DEFAULT.toRule())
                .addRuleInstance(ProjectToCalcRule.Config.DEFAULT.toRule())
                .addRuleInstance(FilterCalcMergeRule.Config.DEFAULT.toRule())
                .addRuleInstance(ProjectCalcMergeRule.Config.DEFAULT.toRule())
                .addRuleInstance(LogicalRules.CALC_INTO_SCAN_RULE);

        return hepProgramBuilder.build();
    }

    public static RelNode transformProjectAndFilterIntoCalc(RelNode rel) {
        HepPlanner planner = new HepPlanner(
                HEP_CALC_REWRITER_PROGRAM,
                Contexts.empty(),
                true,
                null,
                RelOptCostImpl.FACTORY
        );

        planner.setRoot(rel);
        return planner.findBestExp();
    }
}
