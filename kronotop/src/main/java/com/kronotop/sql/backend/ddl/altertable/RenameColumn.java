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
import com.kronotop.sql.backend.ddl.model.ColumnModel;
import com.kronotop.sql.backend.ddl.model.TableModel;
import com.kronotop.sql.parser.SqlAlterTable;

import java.util.List;

public class RenameColumn extends BaseAlterType implements AlterType {

    public RenameColumn(SqlService service) {
        super(service);
    }

    @Override
    public RedisMessage alter(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable) throws SqlExecutionException {
        String table = service.getTableNameFromNames(sqlAlterTable.name.names);
        String schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        DirectorySubspace subspace = service.openTableSubspace(tr, schema, table);
        TableModel tableModel = service.getLatestTableModel(tr, subspace);

        int index = findColumnIndex(sqlAlterTable.columnName.getSimple(), tableModel);
        ColumnModel column = tableModel.getColumnList().get(index);
        column.setName(sqlAlterTable.newColumnName.getSimple());

        service.saveTableModel(tr, tableModel, subspace);
        return new SimpleStringRedisMessage(Response.OK);
    }

    @Override
    public void notifyCluster(TransactionResult result, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        publishTableAlteredEvent(result, context, sqlAlterTable);
    }
}
