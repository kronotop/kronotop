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
import com.kronotop.protocol.SqlArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.BaseHandlerTest;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.backend.AssertResponse;
import com.kronotop.sql.backend.ddl.model.ColumnModel;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class SqlMetadataServiceIntegrationTest extends BaseHandlerTest {

    @Test
    public void test_SqlMetadata_CREATE_SCHEMA() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql(SqlArgs.Builder.queries("CREATE SCHEMA foobar")).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
        SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
        assertEquals(Response.OK, message.content());

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                sqlMetadataService.findSchemaMetadata("foobar");
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
            sqlMetadataService.findSchemaMetadata("barfoo");
        });
    }

    @Test
    public void test_SqlMetadata_CREATE_TABLE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql(SqlArgs.Builder.queries("CREATE TABLE public.users (age INTEGER)")).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
        SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
        assertEquals(Response.OK, message.content());

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                KronotopTable kronotopTable = sqlMetadataService.findTable("public", "users");
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
            sqlMetadataService.findTable("foobar", "users");
        });
    }

    @Test
    public void test_findTable_TableNotExistsException() {
        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        assertThrows(TableNotExistsException.class, () -> {
            sqlMetadataService.findTable("public", "users");
        });
    }

    @Test
    public void test_DROP_SCHEMA_successfully() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        // public schema is created automatically because it's the default schema in test.conf
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("DROP SCHEMA public")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());

            SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                try {
                    sqlMetadataService.findSchemaMetadata("public");
                } catch (SchemaNotExistsException e) {
                    return true;
                }
                return false;
            });
        }
    }

    @Test
    public void test_DROP_TABLE_successfully() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("CREATE TABLE users (age INTEGER)")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());

            SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                try {
                    sqlMetadataService.findTable("public", "users");
                } catch (TableNotExistsException e) {
                    return false;
                }
                return true;
            });
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("DROP TABLE users")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());

            SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                try {
                    System.out.println(sqlMetadataService.findTable("public", "users"));
                } catch (TableNotExistsException e) {
                    return true;
                }
                return false;
            });
        }
    }

    @Test
    public void test_ALTER_TABLE_then_RENAME_TABLE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("CREATE TABLE public.users (age INTEGER)")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("ALTER TABLE public.users RENAME TO foobar")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                KronotopTable kronotopTable = sqlMetadataService.findTable("public", "foobar");
                assertNotNull(kronotopTable);
            } catch (SchemaNotExistsException | TableNotExistsException e) {
                return false;
            }
            return true;
        });
    }

    @Test
    public void test_ALTER_TABLE_and_ADD_COLUMN() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("CREATE TABLE public.users (age INTEGER)")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("ALTER TABLE public.users ADD COLUMN username VARCHAR(30)")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = kronotopInstance.getContext().getFoundationDB().createTransaction()) {
                TableWithVersion tableWithVersion = getLatestTableVersion(tr, "public", "users");
                for (ColumnModel columnModel : tableWithVersion.getTableModel().getColumnList()) {
                    if (columnModel.getName().equals("username")) {
                        return true;
                    }
                }
                return false;
            }
        });
    }

    @Test
    public void test_ALTER_TABLE_and_DROP_COLUMN() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("CREATE TABLE public.users (age INTEGER, username VARCHAR)")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("ALTER TABLE public.users DROP COLUMN username")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = kronotopInstance.getContext().getFoundationDB().createTransaction()) {
                TableWithVersion tableWithVersion = getLatestTableVersion(tr, "public", "users");
                Set<String> columns = new HashSet<>();
                for (ColumnModel columnModel : tableWithVersion.getTableModel().getColumnList()) {
                    columns.add(columnModel.getName());
                }
                return !columns.contains("username");
            }
        });
    }

    @Test
    public void test_ALTER_TABLE_and_RENAME_COLUMN() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("CREATE TABLE public.users (age INTEGER, username VARCHAR)")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("ALTER TABLE public.users RENAME COLUMN username TO renamedcolumn")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = kronotopInstance.getContext().getFoundationDB().createTransaction()) {
                TableWithVersion tableWithVersion = getLatestTableVersion(tr, "public", "users");
                Set<String> columns = new HashSet<>();
                for (ColumnModel columnModel : tableWithVersion.getTableModel().getColumnList()) {
                    columns.add(columnModel.getName());
                }
                return columns.contains("renamedcolumn");
            }
        });
    }

    @Test
    public void test_SqlMetadata_CREATE_TABLE_KronotopSchema_has_table() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("CREATE TABLE public.users (age INTEGER, username VARCHAR)")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(
                () -> sqlMetadataService.findSchemaMetadata("public").getKronotopSchema().getTableMap().containsKey("users")
        );
    }

    @Test
    public void test_SqlMetadata_DROP_TABLE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("CREATE TABLE public.users (age INTEGER, username VARCHAR)")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("DROP TABLE public.users")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(
                () -> !sqlMetadataService.findSchemaMetadata("public").getKronotopSchema().getTableMap().containsKey("users")
        );
    }

    @Test
    public void test_SqlMetadata_ALTER_TABLE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("CREATE TABLE public.users (age INTEGER, username VARCHAR)")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.queries("ALTER TABLE public.users DROP COLUMN username")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(
                () -> {
                    Table table = sqlMetadataService.findSchemaMetadata("public").getKronotopSchema().getTableMap().get("users");
                    KronotopTable kronotopTable = (KronotopTable) table;
                    List<ColumnModel> columns = kronotopTable.getTableModel().getColumnList();
                    for (ColumnModel column : columns) {
                        if (Objects.equals(column.getName(), "username")) {
                            return false;
                        }
                    }
                    return true;
                }
        );
    }
}