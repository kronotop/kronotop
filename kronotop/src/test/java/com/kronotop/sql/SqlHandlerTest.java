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

import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.protocol.SqlArgs;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Request;
import com.kronotop.server.impl.RespRequest;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.backend.AssertResponse;
import com.kronotop.sql.protocol.SqlMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlHandlerTest extends BaseHandlerTest {

    @Test
    public void test_SqlParserException() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql(SqlArgs.Builder.queries("CREATE SCHEMA")).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<ErrorRedisMessage> assertResponse = new AssertResponse<>();
        ErrorRedisMessage message = assertResponse.getMessage(response, 0, 1);
        // javacc-maven-plugin > 2.4 breaks the parser's error handling. This check added here to control it.
        assertEquals("SQL Incorrect syntax near the keyword 'SCHEMA' at line 1, column 8.", message.content());
    }

    @Test
    public void test_SqlMessage_getQueries() {
        List<RedisMessage> messages = new ArrayList<>();
        String queryOne = "INSERT INTO users(integer_column, varchar_column)";
        String queryTwo = "INSERT INTO users(double_column, varchar_column)";

        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("SQL".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes(queryOne.getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes(queryTwo.getBytes())));

        ChannelHandlerContext context = new MockChannelHandlerContext(channel);
        Request request = new RespRequest(context, new ArrayRedisMessage(messages));
        SqlMessage sqlMessage = new SqlMessage(request);

        assertEquals(2, sqlMessage.getQueries().size());
        assertEquals(0, sqlMessage.getReturning().size());
        assertEquals(queryOne, sqlMessage.getQueries().get(0));
        assertEquals(queryTwo, sqlMessage.getQueries().get(1));
    }

    @Test
    public void test_SqlMessage_getReturning() {
        List<RedisMessage> messages = new ArrayList<>();
        String query = "INSERT INTO users(integer_column, varchar_column)";

        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("SQL".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes(query.getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("RETURNING".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("id".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("integer_column".getBytes())));

        ChannelHandlerContext context = new MockChannelHandlerContext(channel);
        Request request = new RespRequest(context, new ArrayRedisMessage(messages));
        SqlMessage sqlMessage = new SqlMessage(request);

        assertEquals(1, sqlMessage.getQueries().size());
        assertEquals(2, sqlMessage.getReturning().size());
        assertEquals(query, sqlMessage.getQueries().get(0));
        assertTrue(sqlMessage.getReturning().contains("id"));
        assertTrue(sqlMessage.getReturning().contains("integer_column"));
    }
}