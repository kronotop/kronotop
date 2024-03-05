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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.core.Context;
import com.kronotop.core.NamespaceUtils;
import com.kronotop.core.TransactionUtils;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.server.ChannelAttributes;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.backend.metadata.SchemaNotExistsException;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.backend.metadata.VersionedTableMetadata;
import com.kronotop.sql.optimizer.enumerable.EnumerableTableModify;
import com.kronotop.sql.plan.PlanContext;
import com.kronotop.sql.plan.Row;
import com.kronotop.sql.serialization.IntegerSerializer;
import com.kronotop.sql.serialization.StringSerializer;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class EnumerableTableModifyVisitor extends BaseVisitor {

    public EnumerableTableModifyVisitor(Context context) {
        super(context);
    }

    private KronotopTable getLatestKronotopTable(PlanContext planContext, EnumerableTableModify node) throws SqlExecutionException {
        List<String> qualifiedName = node.getTable().getQualifiedName();
        String schema = qualifiedName.get(0);
        String table = qualifiedName.get(1);
        try {
            VersionedTableMetadata versionedTableMetadata = sqlMetadata.findVersionedTableMetadata(schema, table);
            String tableVersion = versionedTableMetadata.getLatestVersion();
            planContext.setTableVersion(tableVersion);
            return versionedTableMetadata.get(tableVersion);
        } catch (SchemaNotExistsException | TableNotExistsException e) {
            throw new SqlExecutionException(e);
        }
    }

    @SuppressWarnings("ConstantConditions")
    private List<byte[]> encodeRow(EnumerableTableModify node, Row row) {
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

    private void insertOperation(PlanContext planContext, List<byte[]> encodedRow) {
        Transaction tr = planContext.getTransaction();
        Namespace namespace = planContext.getNamespace();

        int userVersion = planContext.getUserVersion();

        // namespace | sql | table-prefix |
        Subspace subspace = namespace.getSql().subspace(Tuple.fromBytes(planContext.getKronotopTable().getPrefix()));

        BitSet bitSet = new BitSet();
        for (int index = 0; index < encodedRow.size(); index++) {
            byte[] field = encodedRow.get(index);
            if (field == null) {
                continue;
            }
            bitSet.set(index + 1);
            Tuple tuple = Tuple.from(Versionstamp.incomplete(userVersion), index + 1);
            // namespace | sql | table-prefix | $VERSIONSTAMP | $USER_VERSION | $INDEX+1
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), field);
        }

        // namespace | sql | table-prefix | $VERSIONSTAMP | $USER_VERSION | 0
        byte[] fields = bitSet.toByteArray();
        byte[] tableVersion = planContext.getTableVersion().getBytes();
        ByteBuffer buf = ByteBuffer.allocate(tableVersion.length + fields.length);
        buf.put(tableVersion);
        buf.put(fields);
        Tuple tuple = Tuple.from(Versionstamp.incomplete(userVersion), 0);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), buf.array());
    }

    public void visit(PlanContext planContext, EnumerableTableModify node) throws SqlExecutionException {
        Transaction tr = TransactionUtils.getOrCreateTransaction(context, planContext.getChannelContext());
        planContext.setTransaction(tr);

        Namespace namespace = NamespaceUtils.open(context, planContext.getChannelContext(), tr);
        planContext.setNamespace(namespace);

        KronotopTable kronotopTable = getLatestKronotopTable(planContext, node);
        planContext.setKronotopTable(kronotopTable);

        for (Row row : planContext.getRows()) {
            List<byte[]> encodedRow = encodeRow(node, row);
            switch (node.getOperation()) {
                case INSERT -> insertOperation(planContext, encodedRow);
                default -> throw new SqlExecutionException("Unknown table modify operation: " + node.getOperation());
            }
        }

        TransactionUtils.commitIfOneOff(tr, planContext.getChannelContext());
        planContext.setResponse(new SimpleStringRedisMessage(Response.OK));
    }
}
