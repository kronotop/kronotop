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

package com.kronotop.sql.ddl;

import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.protocol.SqlArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.BaseHandlerTest;
import com.kronotop.sql.AssertResponse;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class CreateSchemaTest extends BaseHandlerTest {
    @Test
    public void test_executeSuccessfully() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql(SqlArgs.Builder.query("CREATE SCHEMA foobar")).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
        SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
        assertEquals(Response.OK, message.content());
    }

    @Test
    public void test_SubSchema_executeSuccessfully() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql(SqlArgs.Builder.query("CREATE SCHEMA foobar.barfoo")).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<ErrorRedisMessage> assertResponse = new AssertResponse<>();
        ErrorRedisMessage message = assertResponse.getMessage(response, 0, 1);
        assertEquals("SQL Sub-schemas are not allowed", message.content());
    }

    @Test
    public void test_schemaAlreadyExists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.query("CREATE SCHEMA foobar")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
            SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals(Response.OK, message.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sql(SqlArgs.Builder.query("CREATE SCHEMA foobar")).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<ErrorRedisMessage> assertResponse = new AssertResponse<>();
            ErrorRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals("SQL Schema 'foobar' already exists", message.content());
        }
    }
    // TODO: Add test for IF NOT EXISTS keyword
}