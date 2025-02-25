/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.redis.handlers.string;


import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.handlers.BaseRedisHandlerTest;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class SetNXHandlerTest extends BaseRedisHandlerTest {

    @Test
    public void test_SETNX() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setnx("mykey", "myvalue").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(IntegerRedisMessage.class, msg);
        IntegerRedisMessage actualMessage = (IntegerRedisMessage) msg;
        assertEquals(1, actualMessage.value());
    }

    @Test
    public void test_SETNX_GET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setnx("mykey", "myvalue").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(IntegerRedisMessage.class, msg);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) msg;
            assertEquals(1, actualMessage.value());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("mykey").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(FullBulkStringRedisMessage.class, msg);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
            assertEquals("myvalue", actualMessage.content().toString(CharsetUtil.US_ASCII));
        }
    }

    @Test
    public void test_SETNX_ExistingKey() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("mykey", "myvalue").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setnx("mykey", "myvalue-2").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(IntegerRedisMessage.class, msg);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) msg;
            assertEquals(0, actualMessage.value());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("mykey").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(FullBulkStringRedisMessage.class, msg);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
            assertEquals("myvalue", actualMessage.content().toString(CharsetUtil.US_ASCII));
        }
    }
}