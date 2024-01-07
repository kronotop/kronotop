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

package com.kronotop.sql;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

/**
 * The Parser class provides methods for parsing SQL queries and returning the corresponding SqlNode.
 */
public class Parser {
    private static final SqlParser.Config parserConfig = getParserConfig();

    private static SqlParser.Config getParserConfig() {
        SqlParser.Config parserConfig = SqlParser.config().
                withParserFactory(SqlDdlParserImpl.FACTORY);
        return SqlDialect.DatabaseProduct.POSTGRESQL.getDialect().configureParser(parserConfig);
    }

    /**
     * Parses the given SQL query and returns the corresponding SqlNode.
     *
     * @param query The SQL query to be parsed
     * @return The SQL node representing the parsed query
     * @throws SqlParseException If an error occurs while parsing the query
     */
    public static SqlNode parse(String query) throws SqlParseException {
        SqlParser parser = SqlParser.create(query, parserConfig);
        return parser.parseStmt();
    }
}
