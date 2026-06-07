/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.zmap.handlers;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ZSetI64HandlerTest extends BaseHandlerTest {

    @Test
    void shouldSetAndGetValue() {
        // Behavior: ZSET.I64 stores a value that ZGET.I64 can read back.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("key", 42).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(42, ((IntegerRedisMessage) response).value());
        }
    }

    @Test
    void shouldOverwriteExistingValue() {
        // Behavior: A second ZSET.I64 overwrites the first value.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("key", 100).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("key", 200).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(200, ((IntegerRedisMessage) response).value());
        }
    }

    @Test
    void shouldSetWithinTransaction() {
        // Behavior: ZSET.I64 within BEGIN/COMMIT persists the value.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        KronotopCommandBuilder<String, String> kronotopCommandBuilder = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            kronotopCommandBuilder.begin().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("tx-key", 99).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            kronotopCommandBuilder.commit().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("tx-key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(99, ((IntegerRedisMessage) response).value());
        }
    }

    @Test
    void shouldInteropWithZinc() {
        // Behavior: ZSET.I64 then ZINC.I64 produces the sum readable via ZGET.I64.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("counter", 100).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zinci64("counter", 50).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("counter").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(150, ((IntegerRedisMessage) response).value());
        }
    }

    @Test
    void shouldSetNegativeValue() {
        // Behavior: ZSET.I64 stores negative values correctly.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("key", -12345).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(-12345, ((IntegerRedisMessage) response).value());
        }
    }

    @Test
    void shouldSetZero() {
        // Behavior: ZSET.I64 stores zero correctly.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("key", 0).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(0, ((IntegerRedisMessage) response).value());
        }
    }

    @Test
    void shouldSetLongMaxValue() {
        // Behavior: ZSET.I64 stores Long.MAX_VALUE correctly.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("key", Long.MAX_VALUE).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(Long.MAX_VALUE, ((IntegerRedisMessage) response).value());
        }
    }

    @Test
    void shouldSetLongMinValue() {
        // Behavior: ZSET.I64 stores Long.MIN_VALUE correctly.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("key", Long.MIN_VALUE).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(Long.MIN_VALUE, ((IntegerRedisMessage) response).value());
        }
    }
}
