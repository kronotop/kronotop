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

package com.kronotop.sql.backend.metadata;

import com.apple.foundationdb.Transaction;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.BaseHandlerTest;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.backend.ddl.model.ColumnModel;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class SqlMetadataServiceIntegrationTest extends BaseHandlerTest {

    @Test
    public void test_createSchemaSuccessfully() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql("CREATE SCHEMA public.foobar.barfoo").encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, actualMessage.content());

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                sqlMetadataService.findSchema(List.of("public", "foobar", "barfoo"));
            } catch (SchemaNotExistsException e) {
                return false;
            }
            return true;
        });
    }

    @Test
    public void test_findSchema_SchemaNotExistsException() {
        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        assertThrows(SchemaNotExistsException.class, () -> {
            sqlMetadataService.findSchema(List.of("public", "foobar", "barfoo"));
        });
    }

    @Test
    public void test_createTableSuccessfully() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql("CREATE TABLE public.users (age INTEGER)").encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, actualMessage.content());

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                KronotopTable kronotopTable = sqlMetadataService.findTable(List.of("public"), "users");
                assertNotNull(kronotopTable);
            } catch (SchemaNotExistsException | TableNotExistsException e) {
                return false;
            }
            return true;
        });
    }

    @Test
    public void test_findTable_SchemaNotExistsException() {
        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        assertThrows(SchemaNotExistsException.class, () -> {
            sqlMetadataService.findTable(List.of("public", "foobar"), "users");
        });
    }

    @Test
    public void test_findTable_TableNotExistsException() {
        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        assertThrows(TableNotExistsException.class, () -> {
            sqlMetadataService.findTable(List.of("public"), "users");
        });
    }

    @Test
    public void test_dropSchemaSuccessfully() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("CREATE SCHEMA public.foobar.barfoo").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                try {
                    sqlMetadataService.findSchema(List.of("public", "foobar", "barfoo"));
                } catch (SchemaNotExistsException e) {
                    return false;
                }
                return true;
            });
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("DROP SCHEMA public.foobar.barfoo").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                try {
                    sqlMetadataService.findSchema(List.of("public", "foobar", "barfoo"));
                } catch (SchemaNotExistsException e) {
                    return true;
                }
                return false;
            });
        }
    }

    @Test
    public void test_dropTableSuccessfully() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("CREATE TABLE users (age INTEGER)").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                try {
                    sqlMetadataService.findTable(List.of("public"), "users");
                } catch (TableNotExistsException e) {
                    return false;
                }
                return true;
            });
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("DROP TABLE users").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                try {
                    System.out.println(sqlMetadataService.findTable(List.of("public"), "users"));
                } catch (TableNotExistsException e) {
                    return true;
                }
                return false;
            });
        }
    }

    @Test
    public void test_alterTable_renameTable() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("CREATE TABLE public.users (age INTEGER)").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("ALTER TABLE public.users RENAME TO foobar").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                KronotopTable kronotopTable = sqlMetadataService.findTable(List.of("public"), "foobar");
                assertNotNull(kronotopTable);
            } catch (SchemaNotExistsException | TableNotExistsException e) {
                return false;
            }
            return true;
        });
    }

    @Test
    public void test_alterTable_addColumn() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("CREATE TABLE public.users (age INTEGER)").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("ALTER TABLE public.users ADD COLUMN username VARCHAR(30)").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = kronotopInstance.getContext().getFoundationDB().createTransaction()) {
                TableWithVersion tableWithVersion = sqlMetadataService.getLatestTableVersion(tr, List.of("public"), "users");
                for (ColumnModel columnModel : tableWithVersion.getTableModel().getColumnList()) {
                    if (columnModel.getNames().get(0).equals("username")) {
                        return true;
                    }
                }
                return false;
            }
        });
    }

    @Test
    public void test_alterTable_dropColumn() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("CREATE TABLE public.users (age INTEGER, username VARCHAR)").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql("ALTER TABLE public.users DROP COLUMN username").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = kronotopInstance.getContext().getFoundationDB().createTransaction()) {
                TableWithVersion tableWithVersion = sqlMetadataService.getLatestTableVersion(tr, List.of("public"), "users");
                Set<String> columns = new HashSet<>();
                for (ColumnModel columnModel : tableWithVersion.getTableModel().getColumnList()) {
                    columns.add(columnModel.getNames().get(0));
                }
                return !columns.contains("username");
            }
        });
    }
}