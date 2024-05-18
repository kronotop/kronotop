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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.core.Context;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.executor.PlanContext;
import com.kronotop.sql.executor.Row;
import com.kronotop.sql.optimizer.physical.PhysicalTableModify;
import com.kronotop.sql.serialization.IntegerSerializer;
import com.kronotop.sql.serialization.StringSerializer;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class PhysicalTableModifyVisitor extends BaseVisitor {

    public PhysicalTableModifyVisitor(Context context) {
        super(context);
    }

    @SuppressWarnings("ConstantConditions")
    private List<byte[]> encodeRow(PhysicalTableModify node, Row<RexLiteral> row) {
        List<byte[]> encodedRow = new ArrayList<>();
        for (RelDataTypeField field : node.getTable().getRowType().getFieldList()) {
            RexLiteral rexLiteral = row.get(field.getName());
            if (rexLiteral == null) {
                encodedRow.add(null);
                continue;
            }
            switch (rexLiteral.getType().getSqlTypeName()) {
                case TINYINT -> {
                    byte value = rexLiteral.getValueAs(Byte.class);
                    encodedRow.add(new byte[]{value});
                }
                case INTEGER -> {
                    Integer value = rexLiteral.getValueAs(Integer.class);
                    assert value != null;
                    encodedRow.add(IntegerSerializer.serialize(value));
                }
                case CHAR -> {
                    String value = rexLiteral.getValueAs(String.class);
                    assert value != null;
                    encodedRow.add(StringSerializer.serialize(value));
                }
            }
        }
        return encodedRow;
    }

    private void insertOperation(PlanContext planContext, PhysicalTableModify node, List<byte[]> encodedRow) {
        Transaction tr = getTransaction(planContext);
        Namespace namespace = getNamespace(planContext);

        int userVersion = planContext.getAndIncrementUserVersion();

        // namespace | sql | table-prefix |
        Subspace subspace = openTableSubspace(planContext, namespace);

        BitSet bitSet = new BitSet();
        for (int index = 0; index < encodedRow.size(); index++) {
            byte[] field = encodedRow.get(index);
            if (field == null) {
                // ID is a computed and stored field.
                RelDataTypeField dataTypeField = node.getTable().getRowType().getFieldList().get(index);
                if (dataTypeField.getName().equals("id")) {
                    field = new byte[0];
                }
            }

            // Don't set the field if it is null.
            if (field == null) {
                continue;
            }
            int fdbIndex = index+1;
            bitSet.set(fdbIndex);
            Tuple tuple = Tuple.from(Versionstamp.incomplete(userVersion), fdbIndex);
            // namespace | sql | table-prefix | $VERSIONSTAMP | $USER_VERSION | $FDB_INDEX
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), field);
        }

        setRecordHeader(planContext, subspace, bitSet);
    }

    public void visit(PlanContext planContext, PhysicalTableModify node) throws SqlExecutionException {
        getLatestKronotopTable(planContext, node);
        Transaction tr = getTransaction(planContext);
        for (Row<RexLiteral> row : planContext.getRexLiterals()) {
            List<byte[]> encodedRow = encodeRow(node, row);
            switch (node.getOperation()) {
                case INSERT -> insertOperation(planContext, node, encodedRow);
                default -> throw new SqlExecutionException("Unknown table modify operation: " + node.getOperation());
            }
        }

        planContext.setMutated(true);
        planContext.setVersionstamp(tr.getVersionstamp());
    }
}
