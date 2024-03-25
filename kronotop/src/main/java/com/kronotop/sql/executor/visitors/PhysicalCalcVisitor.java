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

package com.kronotop.sql.executor.visitors;

import com.kronotop.core.Context;
import com.kronotop.sql.executor.PlanContext;
import com.kronotop.sql.executor.Row;
import com.kronotop.sql.optimizer.physical.PhysicalCalc;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

public class PhysicalCalcVisitor extends BaseVisitor {
    public PhysicalCalcVisitor(Context context) {
        super(context);
    }

    public void visit(PlanContext planContext, PhysicalCalc node) {
        RexProgram program = node.getProgram();
        assert program != null;

        for (int outputIndex = 0; outputIndex < program.getProjectList().size(); outputIndex++) {
            RexLocalRef rexLocalRef = program.getProjectList().get(outputIndex);
            int inputIndex = rexLocalRef.getIndex();
            RelDataTypeField outputField = program.getOutputRowType().getFieldList().get(outputIndex);

            if (inputIndex < program.getInputRowType().getFieldList().size()) {
                RelDataTypeField inputField = program.getInputRowType().getFieldList().get(inputIndex);
                if (!inputField.getName().equals(outputField.getName())) {
                    for (Row<RexLiteral> row : planContext.getRexLiterals()) {
                        row.rename(inputField.getName(), outputField.getName(), outputIndex);
                    }
                }
            }
        }

        for (int outputIndex = 0; outputIndex < program.getProjectList().size(); outputIndex++) {
            RexLocalRef rexLocalRef = program.getProjectList().get(outputIndex);
            int inputIndex = rexLocalRef.getIndex();

            RelDataTypeField outputField = program.getOutputRowType().getFieldList().get(outputIndex);
            if (inputIndex >= program.getInputRowType().getFieldList().size()) {
                // Missing NOT NULL field. Check the name.
                if (outputField.getName().equals(ID_COLUMN_NAME)) {
                    for (Row<RexLiteral> row : planContext.getRexLiterals()) {
                        row.put(outputField.getName(), outputIndex, ID_REXLITERAL);
                    }
                    continue;
                }
                RexNode rexNode = program.getExprList().get(inputIndex);
                if (rexNode instanceof RexLiteral rexLiteral) {
                    for (Row<RexLiteral> row : planContext.getRexLiterals()) {
                        row.put(outputField.getName(), outputIndex, rexLiteral);
                    }
                }
            }
        }
    }
}
