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

package com.kronotop.foundationdb.zmap;

import com.kronotop.BaseHandlerTest;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.DoubleRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ZGetF64HandlerTest extends BaseHandlerTest {

    @Test
    void shouldGetExistingValue() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set value via ZINC.F64
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("my-counter", 123.456).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Get value via ZGET.F64
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("my-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(123.456, actualMessage.value(), 0.0001);
        }
    }

    @Test
    void shouldRejectInvalidStoredValueSize() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Store a non-8-byte value using ZSET
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset("invalid-counter", "abc").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Try to read it with ZGET.F64 - should fail
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("invalid-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals("ERR Invalid stored value: expected 8-byte IEEE-754 double", actualMessage.content());
        }
    }

    @Test
    void shouldReturnNilForNonExistentKey() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        cmd.zgetf64("non-existent-key").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, response);
        FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
        assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
    }

    @Test
    void shouldGetValueAfterMultipleIncrements() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Multiple increments
        for (int i = 0; i < 10; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("multi-counter", 2.5).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Verify total (2.5 * 10 = 25.0)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("multi-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(25.0, actualMessage.value(), 0.0001);
        }
    }

    @Test
    void shouldGetNegativeValue() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set negative value directly
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("negative-counter", -99.99).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Verify negative value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("negative-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(-99.99, actualMessage.value(), 0.0001);
        }
    }

    @Test
    void shouldGetZeroValue() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Increment by 50.5
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("zero-counter", 50.5).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Decrement by 50.5
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("zero-counter", -50.5).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Verify zero
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("zero-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(0.0, actualMessage.value(), 0.0001);
        }
    }

    @Test
    void shouldGetFractionalValue() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set a precise fractional value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("fractional-counter", 3.14159265359).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Verify the fractional value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("fractional-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(3.14159265359, actualMessage.value(), 0.00000000001);
        }
    }
}
