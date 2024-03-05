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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.core.NamespaceUtils;
import com.kronotop.core.TransactionUtils;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.backend.AssertResponse;
import com.kronotop.sql.backend.metadata.SchemaNotExistsException;
import com.kronotop.sql.backend.metadata.SqlMetadataService;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class SqlIntegrationTest extends BaseSqlTest {

    public void createTable(String table, String query) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql(query).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
        SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
        assertEquals(Response.OK, message.content());

        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                KronotopTable kronotopTable = sqlMetadataService.findTable("public", table);
                assertNotNull(kronotopTable);
            } catch (SchemaNotExistsException | TableNotExistsException e) {
                return false;
            }
            return true;
        });
    }

    @Test
    public void test_INSERT_INTO_single_value() {
        createTable("users", "CREATE TABLE users (age INTEGER, username VARCHAR)");

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql("INSERT INTO users (age, username) VALUES (35, 'foobar')").encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
        SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
        assertEquals(Response.OK, message.content());

        // Check the inserted values, ignore key encoding.
        ChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);
        Transaction tr = TransactionUtils.getOrCreateTransaction(kronotopInstance.getContext(), channelHandlerContext);
        Namespace namespace = NamespaceUtils.open(kronotopInstance.getContext(), channelHandlerContext, tr);

        int total = 0;
        Subspace subspace = namespace.getSql();
        for (KeyValue item : tr.getRange(subspace.range(), 10)) {
            total++;
        }
        assertEquals(3, total);
    }

    @Test
    public void test_INSERT_INTO_multiple_values() {
        createTable("users", "CREATE TABLE users (age INTEGER, username VARCHAR)");

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql("INSERT INTO users (username, age) VALUES ('foobar', 35), ('barfoo', 36)").encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
        SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
        assertEquals(Response.OK, message.content());

        // Check the inserted values, ignore key encoding.
        ChannelHandlerContext channelHandlerContext = new MockChannelHandlerContext(channel);
        Transaction tr = TransactionUtils.getOrCreateTransaction(kronotopInstance.getContext(), channelHandlerContext);
        Namespace namespace = NamespaceUtils.open(kronotopInstance.getContext(), channelHandlerContext, tr);

        int total = 0;
        Subspace subspace = namespace.getSql();
        for (KeyValue item : tr.getRange(subspace.range(), 10)) {
            total++;
        }
        assertEquals(6, total);
    }
}
