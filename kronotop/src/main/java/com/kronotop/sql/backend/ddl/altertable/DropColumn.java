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
import com.kronotop.sql.backend.ddl.model.TableModel;
import com.kronotop.sql.parser.SqlAlterTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

public class DropColumn extends BaseAlterType implements AlterType {

    public DropColumn(SqlService service) {
        super(service);
    }

    @Override
    public RedisMessage alter(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable) throws SqlExecutionException {
        assert sqlAlterTable.columnList != null;

        String table = service.getTableNameFromNames(sqlAlterTable.name.names);
        String schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        DirectorySubspace subspace = service.openTableSubspace(tr, schema, table);
        TableModel tableModel = service.getLatestTableModel(tr, subspace);

        List<Integer> indexes = new ArrayList<>();
        for (SqlNode column : sqlAlterTable.columnList.getList()) {
            SqlIdentifier identifier = (SqlIdentifier) column;
            assert identifier != null;
            int index = findColumnIndex(identifier.getSimple(), tableModel);
            indexes.add(index);
        }
        for (int index : indexes) {
            tableModel.getColumnList().remove(index);
        }

        service.saveTableModel(tr, tableModel, subspace);
        return new SimpleStringRedisMessage(Response.OK);
    }

    @Override
    public void notifyCluster(TransactionResult result, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        publishTableAlteredEvent(result, context, sqlAlterTable);
    }
}
