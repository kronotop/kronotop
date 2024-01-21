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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

public class KronotopSqlDdlNodes {
    public static SqlAlterTable alterTable(SqlParserPos pos, SqlIdentifier name,
                                           SqlAlterTable.AlterType alterType,
                                           SqlIdentifier newTableName,
                                           SqlIdentifier columnName,
                                           SqlIdentifier newColumnName,
                                           @Nullable SqlNodeList columnList) {
        return new SqlAlterTable(pos, name, alterType, newTableName, columnName, newColumnName, columnList);
    }
}
