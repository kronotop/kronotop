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

package com.kronotop.sql.optimizer.logical;

import com.kronotop.sql.KronotopTable;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.rex.RexUtil.EXECUTOR;

@Value.Enclosing
public class CalcIntoScanRule extends RelRule<RelRule.Config> implements TransformationRule {

    private CalcIntoScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        LogicalTableScan scan = call.rel(1);

        final KronotopTable table = scan.getTable().unwrap(KronotopTable.class);
        RexProgram calcProgram = calc.getProgram();

        KronotopTable newTable = table;
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        RexSimplify rexSimplify = new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, EXECUTOR);

        if (calcProgram.getCondition() != null) {
            RexNode calcFilter = calcProgram.expandLocalRef(calcProgram.getCondition());
            assert table != null;
            RexNode scanFilter = table.getFilter();

            List<RexNode> supportedParts = new ArrayList<>(RelOptUtil.conjunctions(calcFilter));

            supportedParts.add(scanFilter);
            RexNode simplifiedCondition = rexSimplify.simplifyFilterPredicates(supportedParts);
            if (simplifiedCondition == null) {
                simplifiedCondition = rexBuilder.makeLiteral(false);
            }
            newTable = newTable.withFilter(simplifiedCondition);
        }

        List<RexNode> newProjects = calcProgram.expandList(calcProgram.getProjectList());

        assert newTable != null;
        newTable = newTable.withProjects(newProjects, calcProgram.getOutputRowType());

        RelOptTableImpl convertedTable = RelOptTableImpl.create(
                scan.getTable().getRelOptSchema(),
                newTable.getRowType(scan.getCluster().getTypeFactory()),
                scan.getTable().getQualifiedName(), newTable, (org.apache.calcite.linq4j.tree.Expression) null
        );

        LogicalTableScan newScan = LogicalTableScan.create(scan.getCluster(), convertedTable, scan.getHints());
        call.transformTo(newScan);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableCalcIntoScanRule.Config.builder()
                .description(CalcIntoScanRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(LogicalCalc.class)
                        .inputs(b1 -> b1
                                .operand(LogicalTableScan.class)
                                .noInputs()))
                .build();

        @Override
        default CalcIntoScanRule toRule() {
            return new CalcIntoScanRule(this);
        }
    }
}
