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

import com.google.common.collect.ImmutableList;
import com.kronotop.core.Context;
import com.kronotop.sql.optimizer.enumerable.EnumerableValues;
import com.kronotop.sql.plan.PlanContext;
import com.kronotop.sql.plan.Row;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

/**
 * The EnumerableValuesVisitor class is responsible for visiting an EnumerableValues node and
 * inserting serialized rows into the PlanContext.
 */
public class EnumerableValuesVisitor extends BaseVisitor{

    public EnumerableValuesVisitor(Context context) {
        super(context);
    }

    /**
     * Visits an {@link EnumerableValues} node and inserts serialized rows into the {@link PlanContext}.
     *
     * @param planContext the context in which the plan is executed
     * @param node        the {@link EnumerableValues} node to be visited
     */
    public void visit(PlanContext planContext, EnumerableValues node) {
        // Currently, we only support the first tuple in the tuples list.
        for (int tupleIndex = 0; tupleIndex < node.getTuples().size(); tupleIndex++) {
            Row row = new Row();
            ImmutableList<RexLiteral> tuple = node.getTuples().get(tupleIndex);
            List<RelDataTypeField> fields = node.getRowType().getFieldList();
            for (int index = 0; index < tuple.size(); index++) {
                RelDataTypeField field = fields.get(index);
                row.put(field.getName(), tuple.get(index));
            }
            planContext.getRows().add(row);
        }
        /*ImmutableList<RexLiteral> tuple = node.getTuples().get(0);

        List<RelDataTypeField> fields = node.getRowType().getFieldList();
        for (int index = 0; index < tuple.size(); index++) {
            RelDataTypeField field = fields.get(index);
            planContext.getRow().put(field.getName(), tuple.get(index));
        }*/
    }
}
