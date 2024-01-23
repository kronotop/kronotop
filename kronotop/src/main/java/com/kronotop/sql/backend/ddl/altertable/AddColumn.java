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

package com.kronotop.sql.backend.ddl.altertable;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.journal.JournalName;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.TransactionResult;
import com.kronotop.sql.backend.ddl.ColumnAlreadyExistsException;
import com.kronotop.sql.backend.ddl.model.ColumnModel;
import com.kronotop.sql.backend.ddl.model.TableModel;
import com.kronotop.sql.backend.metadata.TableAlreadyExistsException;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.TableAlteredEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.parser.SqlAlterTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * AddColumn is a class that represents the ALTER TABLE operation to add a column to a table in the database.
 */
public class AddColumn implements AlterType {
    private final SqlService service;
    private final ObjectMapper objectMapper;

    public AddColumn(SqlService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    /**
     * Adds a column to the specified table in the database.
     *
     * @param tr            the transaction object used to perform the operation
     * @param context       the execution context object containing the schema information
     * @param sqlAlterTable the SqlAlterTable object representing the ALTER TABLE command
     * @return a RedisMessage indicating the success or failure of the operation
     * @throws TableNotExistsException      if the table does not exist in the database
     * @throws TableAlreadyExistsException  if the table already exists in the database
     * @throws ColumnAlreadyExistsException if the column already exists in the table
     */
    public RedisMessage alter(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable)
            throws TableNotExistsException, TableAlreadyExistsException, ColumnAlreadyExistsException {
        if (sqlAlterTable.columnList == null) {
            return new ErrorRedisMessage("column list cannot be empty.");
        }

        String table = service.getTableNameFromNames(sqlAlterTable.name.names);
        List<String> schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        DirectoryLayout tableLayout = service.getMetadataService().getSchemaLayout(schema).tables().add(table);
        try {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, tableLayout.asList()).join();
            AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(), 1, true).iterator();
            if (!iterator.hasNext()) {
                throw new KronotopException(String.format("Table '%s' exists but no version found", table));
            }
            KeyValue next = iterator.next();

            TableModel tableModel = objectMapper.readValue(next.getValue(), TableModel.class);
            LinkedHashMap<String, ColumnModel> columns = new LinkedHashMap<>();
            for (SqlNode column : sqlAlterTable.columnList.getList()) {
                SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) column;
                assert columnDeclaration != null;

                ColumnModel columnModel = new ColumnModel();
                columnModel.setNames(columnDeclaration.name.names);
                SqlTypeName dataType = SqlTypeName.valueOf(columnDeclaration.dataType.getTypeName().names.get(0));
                columnModel.setDataType(dataType);
                columnModel.setStrategy(columnDeclaration.strategy);
                if (columnDeclaration.expression != null) {
                    columnModel.setExpression(columnDeclaration.expression.toString());
                }
                columns.put(columnModel.getNames().get(0), columnModel);
            }

            for (ColumnModel column : tableModel.getColumnList()) {
                String columnName = column.getNames().get(0);
                if (columns.containsKey(columnName)) {
                    throw new ColumnAlreadyExistsException(columnName, table);
                }
            }
            for (ColumnModel column : columns.values()) {
                tableModel.getColumnList().add(column);
            }
            byte[] data = objectMapper.writeValueAsBytes(tableModel);
            Tuple tuple = Tuple.from(Versionstamp.incomplete(), 1);
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), data);
        } catch (IOException e) {
            throw new KronotopException(e.getMessage());
        }
        return new SimpleStringRedisMessage(Response.OK);
    }

    public void notifyCluster(TransactionResult result, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        List<String> schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        String table = service.getTableNameFromNames(sqlAlterTable.name.names);
        byte[] versionstamp = result.getVersionstamp().join();
        byte[] tableVersion = Versionstamp.complete(versionstamp).getBytes();
        TableAlteredEvent columnAddedEvent = new TableAlteredEvent(schema, table, tableVersion);
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.TABLE_ALTERED, columnAddedEvent);
        service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
    }
}
