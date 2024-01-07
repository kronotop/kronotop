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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParserTest {
    /**
     * This test case is responsible for testing the Parser.parse method in
     * situation when a valid SELECT SQL query is provided. We expect no exception
     * to be thrown and a valid SqlNode to be returned.
     */
    @Test
    void testParseSelectQuery() throws SqlParseException {
        String query = "SELECT * FROM users WHERE id = 1";
        SqlNode result = Parser.parse(query);
        assertNotNull(result);
    }

    /**
     * This test case is responsible for testing the Parser.parse method in
     * situation when a valid INSERT SQL query is provided. We expect no exception
     * to be thrown and a valid SqlNode to be returned.
     */
    @Test
    void testParseInsertQuery() throws SqlParseException {
        String query = "INSERT INTO users (id, name) VALUES (1, 'John Doe')";
        SqlNode result = Parser.parse(query);
        assertNotNull(result);
    }

    /**
     * This test case is responsible for testing the Parser.parse method in
     * a situation when an invalid SQL query is provided. We expect SqlParseException
     * to be thrown.
     */
    @Test
    void testParseInvalidQuery() {
        String query = "INSERT INTO users (id, name) VALUES (1, 'John Doe'";  // Missing closing parenthesis
        assertThrows(SqlParseException.class, () -> Parser.parse(query));
    }
}