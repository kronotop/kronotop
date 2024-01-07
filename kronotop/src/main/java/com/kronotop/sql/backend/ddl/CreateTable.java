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

package com.kronotop.sql.backend.ddl;

import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.backend.Executor;
import com.kronotop.sql.backend.FoundationDBBackend;
import com.kronotop.sql.backend.ddl.model.ColumnModel;
import com.kronotop.sql.backend.ddl.model.CreateTableModel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CreateTable extends FoundationDBBackend implements Executor<SqlNode> {

    public CreateTable(SqlService service) {
        super(service);
    }

    private CreateTableModel getCreateTableModel(SqlCreateTable sqlCreateTable) {
        CreateTableModel createTableModel = new CreateTableModel();
        createTableModel.setNames(sqlCreateTable.name.names);
        createTableModel.setOperator(sqlCreateTable.getOperator().kind);
        createTableModel.setQuery(sqlCreateTable.toString());
        createTableModel.setReplace(sqlCreateTable.getReplace());
        createTableModel.setIfNotExists(sqlCreateTable.ifNotExists);

        List<ColumnModel> columnList = new ArrayList<>();
        for (int i = 0; i < Objects.requireNonNull(sqlCreateTable.columnList).size(); i++) {
            SqlNode sqlNode = sqlCreateTable.columnList.get(i);
            SqlColumnDeclaration column = (SqlColumnDeclaration) sqlNode;

            ColumnModel columnModel = new ColumnModel();
            columnModel.setNames(column.name.names);
            columnModel.setStrategy(column.strategy);
            SqlTypeName dataType = SqlTypeName.valueOf(column.dataType.getTypeName().names.get(0));
            columnModel.setDataType(dataType);
            if (column.expression != null) {
                columnModel.setExpression(column.expression.toString());
            }
            columnList.add(columnModel);
        }
        createTableModel.setColumnList(columnList);
        return createTableModel;
    }

    @Override
    public RedisMessage execute(SqlNode node) {
        SqlCreateTable sqlCreateTable = (SqlCreateTable) node;
        if (sqlCreateTable.columnList == null) {
            return new ErrorRedisMessage("column list cannot be empty");
        }
        CreateTableModel createTableModel = getCreateTableModel(sqlCreateTable);
        return new SimpleStringRedisMessage(Response.OK);
    }
}




