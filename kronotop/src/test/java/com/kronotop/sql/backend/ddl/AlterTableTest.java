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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.sql.BaseHandlerTest;
import com.kronotop.sql.backend.ddl.model.ColumnModel;
import com.kronotop.sql.backend.metadata.SqlMetadataService;
import com.kronotop.sql.backend.metadata.TableWithVersion;
import io.lettuce.core.codec.StringCodec;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class AlterTableTest extends BaseHandlerTest {

    @Test
    public void test_RENAME_TABLE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "ALTER TABLE public.users RENAME TO foobar";
        executeSqlQueryReturnsOK(cmd, alterTableQuery);

        SqlMetadataService metadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        try (Transaction tr = kronotopInstance.getContext().getFoundationDB().createTransaction()) {
            List<String> oldTablePath = metadataService.getSchemaLayout("public").tables().add("users").asList();
            assertFalse(DirectoryLayer.getDefault().exists(tr, oldTablePath).join());

            List<String> newTablePath = metadataService.getSchemaLayout("public").tables().add("foobar").asList();
            assertTrue(DirectoryLayer.getDefault().exists(tr, newTablePath).join());
        }
    }

    @Test
    public void test_RENAME_TABLE_OldTableNotExists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String query = "ALTER TABLE public.users RENAME TO foobar";

        ErrorRedisMessage error = executeSqlQueryReturnsError(cmd, query);
        assertEquals("SQL Table 'users' not exists", error.content());
    }

    @Test
    public void test_RENAME_TABLE_SameTable() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "ALTER TABLE public.users RENAME TO users";
        ErrorRedisMessage error = executeSqlQueryReturnsError(cmd, alterTableQuery);
        assertEquals("SQL Table 'users' already exists", error.content());
    }

    @Test
    public void test_RENAME_TABLE_TableAlreadyExists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "CREATE TABLE public.foobar (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, alterTableQuery);


        String failingQuery = "ALTER TABLE public.users RENAME TO foobar";
        ErrorRedisMessage error = executeSqlQueryReturnsError(cmd, failingQuery);
        assertEquals("SQL Table 'foobar' already exists", error.content());
    }

    @Test
    public void test_ADD_COLUMN() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "ALTER TABLE public.users ADD COLUMN (age INTEGER, name VARCHAR)";
        executeSqlQueryReturnsOK(cmd, alterTableQuery);

        TableWithVersion latestTableVersion = kronotopInstance.getContext().getFoundationDB().run(tr -> getLatestTableVersion(tr, "public", "users"));

        List<ColumnModel> columns = latestTableVersion.getTableModel().getColumnList();
        Map<String, ColumnModel> items = new HashMap<>();
        for (ColumnModel column : columns) {
            String columnName = column.getName();
            if (columnName.equals("age") || columnName.equals("name")) {
                items.put(columnName, column);
            }
        }

        assertEquals(2, items.size());
        assertTrue(items.containsKey("age"));
        assertTrue(items.containsKey("name"));

        // TODO: How do we can test expression?
        ColumnModel ageColumn = items.get("age");
        assertEquals("age", ageColumn.getName());
        assertEquals(SqlTypeName.INTEGER, ageColumn.getDataType());
        assertNull(ageColumn.getExpression());
        assertEquals(ColumnStrategy.NULLABLE, ageColumn.getStrategy());

        ColumnModel nameColumn = items.get("name");
        assertEquals("name", nameColumn.getName());
        assertEquals(SqlTypeName.VARCHAR, nameColumn.getDataType());
        assertNull(nameColumn.getExpression());
        assertEquals(ColumnStrategy.NULLABLE, nameColumn.getStrategy());
    }

    @Test
    public void test_ADD_COLUMN_exists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "ALTER TABLE public.users ADD COLUMN username VARCHAR";
        ErrorRedisMessage message = executeSqlQueryReturnsError(cmd, alterTableQuery);
        assertEquals("SQL column 'username' of table 'users' already exists", message.content());
    }

    @Test
    public void test_ADD_COLUMN_NOT_NULL() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "ALTER TABLE public.users ADD COLUMN age INTEGER NOT NULL";
        executeSqlQueryReturnsOK(cmd, alterTableQuery);

        TableWithVersion latestTableVersion = kronotopInstance.getContext().getFoundationDB().run(tr -> getLatestTableVersion(tr, "public", "users"));

        List<ColumnModel> columns = latestTableVersion.getTableModel().getColumnList();
        Map<String, ColumnModel> items = new HashMap<>();
        for (ColumnModel column : columns) {
            String columnName = column.getName();
            if (columnName.equals("age")) {
                items.put(columnName, column);
            }
        }

        assertEquals(1, items.size());
        assertTrue(items.containsKey("age"));

        ColumnModel ageColumn = items.get("age");
        assertEquals("age", ageColumn.getName());
        assertEquals(SqlTypeName.INTEGER, ageColumn.getDataType());
        assertNull(ageColumn.getExpression());
        assertEquals(ColumnStrategy.NOT_NULLABLE, ageColumn.getStrategy());
    }

    @Test
    public void test_DROP_COLUMN() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "ALTER TABLE public.users DROP COLUMN username";
        executeSqlQueryReturnsOK(cmd, alterTableQuery);

        TableWithVersion latestTableVersion = kronotopInstance.getContext().getFoundationDB().run(tr -> getLatestTableVersion(tr, "public", "users"));

        List<ColumnModel> columns = latestTableVersion.getTableModel().getColumnList();
        assertEquals(2, columns.size());
        ColumnModel idColumn = columns.get(1);
        assertEquals("identifier", idColumn.getName());
        assertEquals(SqlTypeName.INTEGER, idColumn.getDataType());
        assertNull(idColumn.getExpression());
        assertEquals(ColumnStrategy.NULLABLE, idColumn.getStrategy());
    }

    @Test
    public void test_DROP_COLUMN_not_exists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "ALTER TABLE public.users DROP COLUMN foobar";
        ErrorRedisMessage message = executeSqlQueryReturnsError(cmd, alterTableQuery);

        assertEquals("SQL column 'foobar' of table 'users' does not exist", message.content());
    }

    @Test
    public void test_RENAME_COLUMN() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "ALTER TABLE public.users RENAME COLUMN username TO renamedcolumn";
        executeSqlQueryReturnsOK(cmd, alterTableQuery);

        TableWithVersion latestTableVersion = kronotopInstance.getContext().getFoundationDB().run(tr -> getLatestTableVersion(tr, "public", "users"));

        List<ColumnModel> columns = latestTableVersion.getTableModel().getColumnList();
        assertEquals(3, columns.size());
        ColumnModel username2Column = columns.get(2);
        assertEquals("renamedcolumn", username2Column.getName());
        assertEquals(SqlTypeName.VARCHAR, username2Column.getDataType());
        assertNull(username2Column.getExpression());
        assertEquals(ColumnStrategy.NULLABLE, username2Column.getStrategy());
    }

    @Test
    public void test_RENAME_COLUMN_not_exists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String createTableQuery = "CREATE TABLE public.users (identifier INTEGER, username VARCHAR)";
        executeSqlQueryReturnsOK(cmd, createTableQuery);

        String alterTableQuery = "ALTER TABLE public.users RENAME COLUMN foobar TO renamedcolumn";
        ErrorRedisMessage message = executeSqlQueryReturnsError(cmd, alterTableQuery);
        assertEquals("SQL column 'foobar' of table 'users' does not exist", message.content());
    }
}