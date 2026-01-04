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
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ZIncD128HandlerTest extends BaseHandlerTest {

    @Test
    void shouldIncrementNewKey() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Increment a non-existent key
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("counter", "10.5").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the value was set
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals("10.5", actualMessage.content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldIncrementExistingKey() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set initial value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("counter", "10.0").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Increment existing key
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("counter", "25.5").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the sum
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals("35.5", actualMessage.content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldDecrementWithNegativeValue() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set initial value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("counter", "100.0").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Decrement with negative value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("counter", "-30.5").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the result
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals("69.5", actualMessage.content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldAccumulateMultipleIncrements() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Multiple increments
        for (int i = 1; i <= 5; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("counter", String.valueOf(i * 1.5)).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the accumulated total (1.5 + 3.0 + 4.5 + 6.0 + 7.5 = 22.5)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals("22.5", actualMessage.content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldIncrementWithinTransaction() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Begin transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Increment within transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("tx-counter", "42.42").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Commit transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify value persisted after commit
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("tx-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals("42.42", actualMessage.content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldHandleHighPrecisionDecimals() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set a high precision value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("precision-counter", "0.123456789012345678901234567890123").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify high precision is preserved
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("precision-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            String result = actualMessage.content().toString(StandardCharsets.UTF_8);
            // Decimal128 has 34 significant digits, verify precision is maintained
            assertEquals("0.123456789012345678901234567890123", result);
        }
    }

    @Test
    void shouldRejectInvalidDecimalInput() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        cmd.zincd128("counter", "not-a-number").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals("ERR ERR invalid decimal", actualMessage.content());
    }

    @Test
    void shouldRejectInvalidStoredValueSize() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Store a non-16-byte value using ZSET
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset("invalid-counter", "abc").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Try to increment it with ZINC.D128 - should fail
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("invalid-counter", "1.0").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals("ERR Invalid stored value: expected 16-byte Decimal128 (IEEE-754 BID)", actualMessage.content());
        }
    }

    @Test
    void shouldHandleScientificNotation() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set a value using scientific notation (positive exponent)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("sci-counter", "1.5E+10").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Add a value with negative exponent
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("sci-counter", "2.5E-5").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the result (15000000000 + 0.000025 = 15000000000.000025)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("sci-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals("15000000000.000025", actualMessage.content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldRejectDecimalOverflow() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Try to set a value that exceeds Decimal128 range
        // Decimal128 max is approximately 9.999999999999999999999999999999999E+6144
        ByteBuf buf = Unpooled.buffer();
        cmd.zincd128("overflow-counter", "1E+6145").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        // Decimal128 throws its own error for out-of-range exponents
        assertEquals("ERR Exponent is out of range for Decimal128 encoding: 6112", actualMessage.content());
    }

    @Test
    void shouldHandleConcurrentIncrementsFromVirtualThreads() throws Exception {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Expected total: 1.0 + 2.0 + 3.0 + 4.0 + 5.0 + 6.0 + 7.0 + 8.0 + 9.0 + 10.0 = 55.0
        String expectedTotal = "55.0";

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();

            // Launch 10 virtual threads, each incrementing with values 1.0-10.0
            for (int i = 1; i <= 10; i++) {
                final String value = i + ".0";
                futures.add(executor.submit(() -> {
                    ByteBuf buf = Unpooled.buffer();
                    cmd.zincd128("concurrent-counter", value).encode(buf);

                    Object response = runCommand(channel, buf);
                    assertInstanceOf(SimpleStringRedisMessage.class, response);
                    SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
                    assertEquals(Response.OK, actualMessage.content());
                }));
            }

            // Wait for all threads to complete
            for (Future<?> future : futures) {
                future.get();
            }
        }

        // Verify the accumulated total
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("concurrent-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals(expectedTotal, actualMessage.content().toString(StandardCharsets.UTF_8));
        }
    }
}
