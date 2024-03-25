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

package com.kronotop.sql.executor;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.core.NamespaceUtils;
import com.kronotop.core.TransactionUtils;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.protocol.CommitArgs;
import com.kronotop.protocol.CommitKeyword;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.protocol.SqlArgs;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.AssertResponse;
import com.kronotop.sql.executor.visitors.BaseVisitor;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class InsertIntoIntegrationTest extends BasePlanIntegrationTest {

    @Test
    public void test_INSERT_INTO_single_value() {
        executeSQLQuery("CREATE TABLE users (age INTEGER, username VARCHAR)");
        awaitSchemaMetadataForTable(DEFAULT_SCHEMA, "users");

        executeSQLQuery("INSERT INTO users (age, username) VALUES (35, 'foobar')");

        // Check the inserted values, ignore key encoding.
        ChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);
        Transaction tr = TransactionUtils.getOrCreateTransaction(kronotopInstance.getContext(), channelHandlerContext);
        Namespace namespace = NamespaceUtils.open(kronotopInstance.getContext(), channelHandlerContext, tr);

        int total = 0;
        Subspace subspace = namespace.getSql();
        for (KeyValue item : tr.getRange(subspace.range(), 10)) {
            total++;
        }
        // 1- Metadata
        // 2- id
        // 3- age
        // 4- username
        assertEquals(4, total);
    }

    @Test
    public void test_INSERT_INTO_multiple_value() {
        executeSQLQuery("CREATE TABLE users (age INTEGER, username VARCHAR)");
        awaitSchemaMetadataForTable(DEFAULT_SCHEMA, "users");

        executeSQLQuery("INSERT INTO users (username, age) VALUES ('foobar', 35), ('barfoo', 36)", 1);

        // Check the inserted values, ignore key encoding.
        ChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);
        Transaction tr = TransactionUtils.getOrCreateTransaction(kronotopInstance.getContext(), channelHandlerContext);
        Namespace namespace = NamespaceUtils.open(kronotopInstance.getContext(), channelHandlerContext, tr);

        int total = 0;
        Subspace subspace = namespace.getSql();
        for (KeyValue item : tr.getRange(subspace.range(), 10)) {
            total++;
        }
        assertEquals(8, total);
    }

    @Test
    public void test_INSERT_INTO_returning() {
        executeSQLQuery("CREATE TABLE users (age INTEGER, username VARCHAR)");
        awaitSchemaMetadataForTable(DEFAULT_SCHEMA, "users");

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql(SqlArgs.Builder.query("INSERT INTO users (age, username) VALUES (35, 'foobar')").returning(BaseVisitor.ID_COLUMN_NAME, "username")).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<MapRedisMessage> assertResponse = new AssertResponse<>();
        MapRedisMessage message = assertResponse.getMessage(response, 0, 1);

        Map<String, RedisMessage> result = new HashMap<>();
        message.children().forEach((key, value) -> {
            String stringKey = ((SimpleStringRedisMessage) key).content();
            result.put(stringKey, value);
        });

        // Check the auto-generated ID
        RedisMessage idValue = result.get(BaseVisitor.ID_COLUMN_NAME);
        assertNotNull(idValue);
        String id = ((FullBulkStringRedisMessage) idValue).content().toString(CharsetUtil.US_ASCII);
        assertNotNull(id);
        assertEquals(16, id.length());


        RedisMessage usernameValue = result.get("username");
        assertNotNull(usernameValue);
        String username = ((FullBulkStringRedisMessage) usernameValue).content().toString(CharsetUtil.US_ASCII);
        assertNotNull(username);
        assertEquals("foobar", username);
    }

    @Test
    public void test_INSERT_INTO_returning_future() {
        executeSQLQuery("CREATE TABLE users (age INTEGER, username VARCHAR)");
        awaitSchemaMetadataForTable(DEFAULT_SCHEMA, "users");

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.query("INSERT INTO users (age, username) VALUES (35, 'foobar')").returning(BaseVisitor.ID_COLUMN_NAME, "username")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<MapRedisMessage> assertResponse = new AssertResponse<>();
            MapRedisMessage message = assertResponse.getMessage(response, 0, 1);

            Map<String, RedisMessage> result = new HashMap<>();
            message.children().forEach((key, value) -> {
                String stringKey = ((SimpleStringRedisMessage) key).content();
                result.put(stringKey, value);
            });

            // Check the future ID
            RedisMessage idValue = result.get(BaseVisitor.ID_COLUMN_NAME);
            assertNotNull(idValue);
            String id = ((FullBulkStringRedisMessage) idValue).content().toString(CharsetUtil.US_ASCII);
            assertNotNull(id);
            assertEquals("$0", id);


            RedisMessage usernameValue = result.get("username");
            assertNotNull(usernameValue);
            String username = ((FullBulkStringRedisMessage) usernameValue).content().toString(CharsetUtil.US_ASCII);
            assertNotNull(username);
            assertEquals("foobar", username);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    public void test_INSERT_INTO_then_COMMIT_RETURNING_FUTURES() {
        executeSQLQuery("CREATE TABLE users (age INTEGER, username VARCHAR)");
        awaitSchemaMetadataForTable(DEFAULT_SCHEMA, "users");

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.query("INSERT INTO users (age, username) VALUES (35, 'foobar')").returning(BaseVisitor.ID_COLUMN_NAME, "username")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<MapRedisMessage> assertResponse = new AssertResponse<>();
            MapRedisMessage message = assertResponse.getMessage(response, 0, 1);

            Map<String, RedisMessage> result = new HashMap<>();
            message.children().forEach((key, value) -> {
                String stringKey = ((SimpleStringRedisMessage) key).content();
                result.put(stringKey, value);
            });

            // Check the future ID
            RedisMessage idValue = result.get(BaseVisitor.ID_COLUMN_NAME);
            assertNotNull(idValue);
            String id = ((FullBulkStringRedisMessage) idValue).content().toString(CharsetUtil.US_ASCII);
            assertNotNull(id);
            assertEquals("$0", id);


            RedisMessage usernameValue = result.get("username");
            assertNotNull(usernameValue);
            String username = ((FullBulkStringRedisMessage) usernameValue).content().toString(CharsetUtil.US_ASCII);
            assertNotNull(username);
            assertEquals("foobar", username);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit(CommitArgs.Builder.returning(CommitKeyword.FUTURES)).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();


            AssertResponse<MapRedisMessage> assertResponse = new AssertResponse<>();
            MapRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertNotNull(message);
            Map<String, RedisMessage> result = new HashMap<>();
            message.children().forEach((key, value) -> {
                String stringKey = ((SimpleStringRedisMessage) key).content();
                result.put(stringKey, value);
            });
            assertTrue(result.containsKey("$0"));
            FullBulkStringRedisMessage idValue = (FullBulkStringRedisMessage) result.get("$0");
            assertNotNull(idValue.content().toString(CharsetUtil.US_ASCII));
        }
    }
}
