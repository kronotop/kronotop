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
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.BaseHandlerTest;
import com.kronotop.sql.backend.metadata.SqlMetadataService;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class AlterTableTest extends BaseHandlerTest {

    @Test
    public void test_RENAME_TABLE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            String query = "CREATE TABLE public.users (id INTEGER, username VARCHAR)";
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(query).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            String query = "ALTER TABLE public.users RENAME TO foobar";
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(query).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        SqlMetadataService metadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        try (Transaction tr = kronotopInstance.getContext().getFoundationDB().createTransaction()) {
            List<String> oldTablePath = metadataService.getSchemaLayout(List.of("public")).tables().add("users").asList();
            assertFalse(DirectoryLayer.getDefault().exists(tr, oldTablePath).join());

            List<String> newTablePath = metadataService.getSchemaLayout(List.of("public")).tables().add("foobar").asList();
            assertTrue(DirectoryLayer.getDefault().exists(tr, newTablePath).join());
        }
    }

    @Test
    public void test_RENAME_TABLE_OldTableNotExists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String query = "ALTER TABLE public.users RENAME TO foobar";
        ByteBuf buf = Unpooled.buffer();
        cmd.sql(query).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals("SQL Table 'users' not exists", actualMessage.content());
    }

    @Test
    public void test_RENAME_TABLE_SameTable() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            String query = "CREATE TABLE public.users (id INTEGER, username VARCHAR)";
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(query).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            String query = "ALTER TABLE public.users RENAME TO users";
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(query).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals("SQL Table 'users' already exists", actualMessage.content());
        }
    }

    @Test
    public void test_RENAME_TABLE_TableAlreadyExists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            String query = "CREATE TABLE public.users (id INTEGER, username VARCHAR)";
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(query).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            String query = "CREATE TABLE public.foobar (id INTEGER, username VARCHAR)";
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(query).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            String query = "ALTER TABLE public.users RENAME TO foobar";
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(query).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals("SQL Table 'foobar' already exists", actualMessage.content());
        }
    }
}