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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.TransactionResult;
import com.kronotop.sql.backend.ddl.ColumnAlreadyExistsException;
import com.kronotop.sql.backend.ddl.model.ColumnModel;
import com.kronotop.sql.backend.ddl.model.TableModel;
import com.kronotop.sql.parser.SqlAlterTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedHashMap;

/**
 * AddColumn is a class that represents the ALTER TABLE operation to add a column to a table in the database.
 */
public class AddColumn extends BaseAlterType implements AlterType {

    public AddColumn(SqlService service) {
        super(service);
    }

    /**
     * Adds a column to the specified table in the database.
     *
     * @param tr            the transaction object used to perform the operation
     * @param context       the execution context object containing the schema information
     * @param sqlAlterTable the SqlAlterTable object representing the ALTER TABLE command
     * @return a RedisMessage indicating the success or failure of the operation
     */
    public RedisMessage alter(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable) throws SqlExecutionException {
        assert sqlAlterTable.columnList != null;

        String table = service.getTableNameFromNames(sqlAlterTable.name.names);
        String schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        DirectorySubspace subspace = service.openTableSubspace(tr, schema, table);
        TableModel tableModel = service.getLatestTableModel(tr, subspace);

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
                throw new SqlExecutionException(new ColumnAlreadyExistsException(columnName, table));
            }
        }
        for (ColumnModel column : columns.values()) {
            tableModel.getColumnList().add(column);
        }

        service.saveTableModel(tr, tableModel, subspace);
        return new SimpleStringRedisMessage(Response.OK);
    }

    public void notifyCluster(TransactionResult result, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        publishTableAlteredEvent(result, context, sqlAlterTable);
    }
}
