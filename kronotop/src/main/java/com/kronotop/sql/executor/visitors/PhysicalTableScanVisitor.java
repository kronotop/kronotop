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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.core.Context;
import com.kronotop.core.VersionstampUtils;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.ddl.model.ColumnModel;
import com.kronotop.sql.executor.PlanContext;
import com.kronotop.sql.metadata.*;
import com.kronotop.sql.optimizer.physical.PhysicalTableScan;
import com.kronotop.sql.serialization.IntegerSerializer;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PhysicalTableScanVisitor extends BaseVisitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalTableScanVisitor.class);

    public PhysicalTableScanVisitor(Context context) {
        super(context);
    }

    private KronotopTable getVersionedTable(String schema, String table, String version) throws TableNotExistsException, SchemaNotExistsException, TableVersionNotExistsException {
        SqlMetadataService sqlMetadataService = context.getService(SqlMetadataService.NAME);
        VersionedTableMetadata versionedTableMetadata = sqlMetadataService.findVersionedTableMetadata(schema, table);
        KronotopTable versionedTable = versionedTableMetadata.get(version);
        if (versionedTable == null) {
            throw new TableVersionNotExistsException(table, version);
        }
        return versionedTable;
    }

    public void visit(PlanContext planContext, PhysicalTableScan node) throws SqlExecutionException {
        Transaction tr = getTransaction(planContext);
        Namespace namespace = getNamespace(planContext);

        KronotopTable table = node.getTable().unwrap(KronotopTable.class);
        assert table != null;

        // namespace | sql | table-prefix
        Subspace subspace = namespace.getSql().subspace(Tuple.fromBytes(table.getPrefix()));

        // Prepare the key selectors: query boundaries.
        KeySelector begin = KeySelector.firstGreaterOrEqual(subspace.pack());
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(subspace.pack()));

        Versionstamp currentKey = null;
        KronotopTable currentTable = null;
        Map<RedisMessage, RedisMessage> row = new HashMap<>();
        AsyncIterable<KeyValue> iterator = tr.getRange(begin, end);
        for (KeyValue keyValue : iterator) {
            Tuple fdbKey = Tuple.fromBytes(keyValue.getKey());

            if (fdbKey.getItems().size() != 5) {
                throw new SqlExecutionException("Key has wrong size: " + fdbKey.getItems().size());
            }

            long fdbIndex = (long) fdbKey.getItems().get(4);

            if (fdbIndex == 0) {
                planContext.getResponse().add(new MapRedisMessage(row));
                row = new HashMap<>();
                currentKey = (Versionstamp) fdbKey.getItems().get(3);
                String tableVersion = new String(keyValue.getValue());
                try {
                    currentTable = getVersionedTable(table.getSchema(), table.getName(), tableVersion);
                } catch (TableNotExistsException | SchemaNotExistsException | TableVersionNotExistsException e) {
                    LOGGER.error("Failed to get versioned table", e);
                    throw new SqlExecutionException("Invalid table entry: " + e.getMessage());
                }
                row.put(
                        new SimpleStringRedisMessage("id"),
                        new SimpleStringRedisMessage(VersionstampUtils.base64Encode(currentKey))
                );
                continue;
            }

            if (fdbIndex >= 1) {
                Objects.requireNonNull(currentKey);
                Objects.requireNonNull(currentTable);
                List<ColumnModel> columns = currentTable.getTableModel().getColumnList();
                ColumnModel column = columns.get((int) fdbIndex - 1);

                RedisMessage name = new SimpleStringRedisMessage(column.getName());
                RedisMessage value = null;
                switch (column.getDataType()) {
                    case SqlTypeName.VARCHAR -> value = new SimpleStringRedisMessage(new String(keyValue.getValue()));
                    case SqlTypeName.INTEGER ->
                            value = new IntegerRedisMessage(IntegerSerializer.deserialize(keyValue.getValue()));
                }
                row.put(name, value);
            }
        }
    }
}
