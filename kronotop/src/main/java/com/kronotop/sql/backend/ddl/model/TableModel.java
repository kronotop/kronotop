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

package com.kronotop.sql.backend.ddl.model;

import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

/**
 * The TableModel class represents a table model that contains information about a SQL table.
 * It includes details such as the operator type, schema, table name, query, column list, and other flags.
 */
public class TableModel {
    private SqlKind operator;
    private String schema;
    private String table;
    private String query;
    private List<ColumnModel> columnList = new ArrayList<>();
    private Boolean replace = false;
    private Boolean ifNotExists = false;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public SqlKind getOperator() {
        return operator;
    }

    public void setOperator(SqlKind operator) {
        this.operator = operator;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public List<ColumnModel> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<ColumnModel> columnList) {
        this.columnList = columnList;
    }

    public Boolean getReplace() {
        return replace;
    }

    public void setReplace(Boolean replace) {
        this.replace = replace;
    }

    public Boolean getIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(Boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }
}