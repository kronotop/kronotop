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

package com.kronotop.sql.plan.visitors;

import com.kronotop.core.Context;
import com.kronotop.sql.optimizer.enumerable.EnumerableProject;
import com.kronotop.sql.plan.PlanContext;
import com.kronotop.sql.plan.Row;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * The EnumerableProjectVisitor class is responsible for visiting a {@link EnumerableProject} node and updating the
 * row state of the {@link PlanContext}.
 */
public class EnumerableProjectVisitor extends BaseVisitor{

    public EnumerableProjectVisitor(Context context) {
        super(context);
    }

    /**
     * Visits the given {@link EnumerableProject} node and updates the row state of the {@link PlanContext}.
     *
     * @param planContext The context of the plan execution
     * @param node        The {@link EnumerableProject} node to visit
     */
    public void visit(PlanContext planContext, EnumerableProject node) {
        List<RelDataTypeField> fields = node.getRowType().getFieldList();

        for (Row row : planContext.getRows()) {
            for (int index = 0; index < node.getProjects().size(); index++) {
                RexNode rexNode = node.getProjects().get(index);
                if (rexNode instanceof RexInputRef rexInputRef) {
                    String alias = String.format("%s%d", SqlUtil.GENERATED_EXPR_ALIAS_PREFIX, rexInputRef.getIndex());
                    if (row.containsKey(alias)) {
                        RexLiteral rexLiteral = row.remove(alias);
                        RelDataTypeField referencedField = fields.get(index);
                        row.put(referencedField.getName(), rexLiteral);
                    }
                } else if (rexNode instanceof RexLiteral rexLiteral) {
                    RelDataTypeField field = fields.get(index);
                    if (SqlTypeName.NULL.equals(rexLiteral.getTypeName())) {
                        row.put(field.getName(), null);
                    } else {
                        row.put(field.getName(), rexLiteral);
                    }
                }
            }
        }
    }
}
