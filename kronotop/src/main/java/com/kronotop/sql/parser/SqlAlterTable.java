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


package com.kronotop.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;
import java.util.List;

/**
 * ALTER TABLE DDL syntax variants:
 * <p>
 * SET OPTIONS:
 * ALTER TABLE <table> [ SET (<option> = <value> [, ... ] ) ]
 * RENAME TABLE
 * ALTER TABLE <table> RENAME TO <new_table>
 * RENAME COLUMN:
 * ALTER TABLE <table> RENAME <column> to <new_column>
 * ADD COLUMN:
 * ALTER TABLE <table>
 * ADD [COLUMN] <column> <type> [NOT NULL] [ENCODING <encodingSpec>];
 * ALTER TABLE <table>
 * ADD (<column> <type> [NOT NULL] [ENCODING <encodingSpec>], ...);
 * ALTER TABLE <table> ADD (<column> <type> DEFAULT <value>);
 * DROP COLUMN:
 * ALTER TABLE <table> DROP COLUMN <column_1>[, <column_2>, ...];
 * ALTER COLUMN:
 * ALTER TABLE <table>
 * ALTER [COLUMN] <column> [SET DATA] TYPE <type> [NOT NULL] [ENCODING
 * <encodingSpec>]
 */


/**
 * Class that encapsulates all information associated with an ALTER TABLE DDL
 * command.
 */
public class SqlAlterTable extends SqlDdl {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE);
    public final SqlIdentifier name;
    public final AlterType alterType;
    public final SqlIdentifier newTableName;
    public final SqlIdentifier columnName;
    public final SqlIdentifier newColumnName;
    public @Nullable SqlNodeList columnList;

    public SqlAlterTable(SqlParserPos pos,
                         SqlIdentifier name,
                         AlterType alterType,
                         SqlIdentifier newTableName,
                         SqlIdentifier columnName,
                         SqlIdentifier newColumnName,
                         @Nullable SqlNodeList columnList) {
        super(OPERATOR, pos);
        this.alterType = alterType;
        this.name = name;
        this.newTableName = newTableName;
        this.columnName = columnName;
        this.newColumnName = newColumnName;
        this.columnList = columnList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        // Add the operands here
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER");
        writer.keyword("TABLE");
        // add other options data here when/as necessary
    }

    public enum AlterType {
        RENAME_TABLE,
        RENAME_COLUMN,
        ADD_COLUMN,
        ALTER_COLUMN,
        DROP_COLUMN,
        ALTER_OPTIONS
    }
}
