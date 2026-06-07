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
import com.kronotop.server.resp3.DoubleRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ZSetF64HandlerTest extends BaseHandlerTest {

    @Test
    void shouldSetAndGetValue() {
        // Behavior: ZSET.F64 stores a value that ZGET.F64 can read back.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetf64("key", 3.14).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            assertEquals(3.14, ((DoubleRedisMessage) response).value(), 0.0001);
        }
    }

    @Test
    void shouldOverwriteExistingValue() {
        // Behavior: A second ZSET.F64 overwrites the first value.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetf64("key", 1.0).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetf64("key", 2.0).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            assertEquals(2.0, ((DoubleRedisMessage) response).value(), 0.0001);
        }
    }

    @Test
    void shouldSetWithinTransaction() {
        // Behavior: ZSET.F64 within BEGIN/COMMIT persists the value.
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
            cmd.zsetf64("tx-key", 99.99).encode(buf);
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
            cmd.zgetf64("tx-key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            assertEquals(99.99, ((DoubleRedisMessage) response).value(), 0.0001);
        }
    }

    @Test
    void shouldInteropWithZinc() {
        // Behavior: ZSET.F64 then ZINC.F64 produces the sum readable via ZGET.F64.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetf64("counter", 10.5).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", 5.5).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("counter").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            assertEquals(16.0, ((DoubleRedisMessage) response).value(), 0.0001);
        }
    }

    @Test
    void shouldNormalizeNegativeZero() {
        // Behavior: ZSET.F64 with -0.0 normalizes to 0.0.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetf64("key", -0.0).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage msg = (DoubleRedisMessage) response;
            assertEquals(0.0, msg.value(), 0.0);
            assertEquals(Double.doubleToRawLongBits(0.0), Double.doubleToRawLongBits(msg.value()));
        }
    }

    @Test
    void shouldRejectNaN() {
        // Behavior: ZSET.F64 rejects NaN values with an error.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        cmd.zsetf64("key", Double.NaN).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
    }

    @Test
    void shouldRejectInfinity() {
        // Behavior: ZSET.F64 rejects Infinity values with an error.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        cmd.zsetf64("key", Double.POSITIVE_INFINITY).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
    }
}
