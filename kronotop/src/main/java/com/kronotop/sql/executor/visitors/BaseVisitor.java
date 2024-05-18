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
import com.kronotop.core.NamespaceUtils;
import com.kronotop.core.TransactionUtils;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.executor.PlanContext;
import com.kronotop.sql.metadata.SchemaNotExistsException;
import com.kronotop.sql.metadata.SqlMetadataService;
import com.kronotop.sql.metadata.TableNotExistsException;
import com.kronotop.sql.metadata.VersionedTableMetadata;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

public class BaseVisitor {
    public static final int RECORD_HEADER_INDEX = 0;
    public static final String ID_COLUMN_NAME = "id";

    protected final RexLiteral ID_REXLITERAL = RexLiteral.fromJdbcString(
            new SqlTypeFactoryImpl(RelDataTypeSystemImpl.DEFAULT).createSqlType(SqlTypeName.CHAR),
            SqlTypeName.CHAR, ID_COLUMN_NAME
    );

    protected Context context;
    protected SqlMetadataService sqlMetadata;

    public BaseVisitor(Context context) {
        this.context = context;
        this.sqlMetadata = context.getService(SqlMetadataService.NAME);
    }

    /**
     * Retrieves the latest version of a KronotopTable based on the given PlanContext and RelNode.
     *
     * @param planContext The PlanContext object containing the execution executor context.
     * @param node        The RelNode representing the table.
     * @return The latest version of the KronotopTable.
     * @throws SqlExecutionException if there is an error executing the SQL.
     */
    protected KronotopTable getLatestKronotopTable(PlanContext planContext, @Nonnull RelNode node) throws SqlExecutionException {
        KronotopTable kronotopTable = planContext.getKronotopTable();
        if (kronotopTable != null) {
            return kronotopTable;
        }
        List<String> qualifiedName = Objects.requireNonNull(node.getTable()).getQualifiedName();
        String schema = qualifiedName.get(0);
        String table = qualifiedName.get(1);
        try {
            VersionedTableMetadata versionedTableMetadata = sqlMetadata.findVersionedTableMetadata(schema, table);
            String tableVersion = versionedTableMetadata.getLatestVersion();
            planContext.setTableVersion(tableVersion);

            kronotopTable = versionedTableMetadata.get(tableVersion);
            planContext.setKronotopTable(kronotopTable);
            return kronotopTable;
        } catch (SchemaNotExistsException | TableNotExistsException e) {
            throw new SqlExecutionException(e);
        }
    }

    /**
     * Retrieves the FDB Transaction for the given PlanContext.
     * If a transaction is already set in the PlanContext, it is returned.
     * Otherwise, a new FDB Transaction is created, set in the PlanContext, and returned.
     *
     * @param planContext The PlanContext object containing the execution executor context.
     * @return The FDB Transaction associated with the PlanContext.
     */
    protected Transaction getTransaction(PlanContext planContext) {
        // Try to find a previously set FDB Transaction first.
        Transaction tr = planContext.getTransaction();
        if (tr != null) {
            return tr;
        }

        // Create a new FDB Transaction and set it to the PlanContext instance.
        tr = TransactionUtils.getOrCreateTransaction(context, planContext.getChannelContext());
        planContext.setTransaction(tr);
        return tr;
    }

    /**
     * Retrieves the Namespace associated with the given PlanContext.
     * If a namespace is already set in the PlanContext, it is returned.
     * Otherwise, a new Namespace is opened, set in
     * the PlanContext, and returned.
     *
     * @param planContext The PlanContext object containing the execution executor context.
     * @return The Namespace associated with the PlanContext.
     */
    protected Namespace getNamespace(PlanContext planContext) {
        // Try to find a previously set Namespace first.
        Namespace namespace = planContext.getNamespace();
        if (namespace != null) {
            return namespace;
        }

        // Open the namespace and set it to the PlanContext instance.
        Transaction tr = getTransaction(planContext);
        namespace = NamespaceUtils.open(context, planContext.getChannelContext(), tr);
        planContext.setNamespace(namespace);
        return namespace;
    }

    /**
     * Opens the table subspace for the given namespace and plan context.
     *
     * @param planContext The PlanContext object containing the execution executor context.
     * @param namespace   The Namespace associated with the PlanContext.
     * @return The Subspace representing the table subspace.
     */
    protected Subspace openTableSubspace(PlanContext planContext, Namespace namespace) {
        // namespace | sql | table-prefix |
        return namespace.getSql().subspace(Tuple.fromBytes(planContext.getKronotopTable().getPrefix()));
    }

    /**
     * Sets the record header for a given PlanContext, Subspace, and BitSet.
     * The record header is calculated based on the provided BitSet and other metadata from the PlanContext object.
     *
     * @param planContext The PlanContext object containing the execution executor context.
     * @param subspace    The Subspace representing the table subspace.
     * @param bitSet      The BitSet indicating the fields that are set in the record.
     */
    protected void setRecordHeader(PlanContext planContext, Subspace subspace, BitSet bitSet) {
        //                                 |        THIS IS THE KEY        |
        // namespace | sql | table-prefix | $VERSIONSTAMP | $USER_VERSION | index(always zero)
        byte[] fields = bitSet.toByteArray();
        byte[] tableVersion = planContext.getTableVersion().getBytes();
        ByteBuffer buf = ByteBuffer.allocate(tableVersion.length + fields.length);
        buf.put(tableVersion);
        buf.put(fields);
        Tuple tuple = Tuple.from(Versionstamp.incomplete(planContext.getUserVersion()), RECORD_HEADER_INDEX);

        Transaction tr = getTransaction(planContext);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), buf.array());
    }
}
