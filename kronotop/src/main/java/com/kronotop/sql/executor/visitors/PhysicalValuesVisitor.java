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

import com.google.common.collect.ImmutableList;
import com.kronotop.core.Context;
import com.kronotop.sql.executor.PlanContext;
import com.kronotop.sql.executor.Row;
import com.kronotop.sql.optimizer.physical.PhysicalValues;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

/**
 * The EnumerableValuesVisitor class is responsible for visiting an EnumerableValues node and
 * inserting serialized rows into the PlanContext.
 */
public class PhysicalValuesVisitor extends BaseVisitor {

    public PhysicalValuesVisitor(Context context) {
        super(context);
    }

    /**
     * Visits an {@link PhysicalValues} node and inserts serialized rows into the {@link PlanContext}.
     *
     * @param planContext the context in which the executor is executed
     * @param node        the {@link PhysicalValues} node to be visited
     */
    public void visit(PlanContext planContext, PhysicalValues node) {
        // Currently, we only support the first tuple in the tuples list.
        for (int tupleIndex = 0; tupleIndex < node.getTuples().size(); tupleIndex++) {
            Row<RexLiteral> row = new Row<>();
            ImmutableList<RexLiteral> tuple = node.getTuples().get(tupleIndex);
            List<RelDataTypeField> fields = node.getRowType().getFieldList();
            for (int index = 0; index < tuple.size(); index++) {
                RelDataTypeField field = fields.get(index);
                row.put(field.getName(), index, tuple.get(index));
            }
            planContext.getRexLiterals().add(row);
        }
    }
}
