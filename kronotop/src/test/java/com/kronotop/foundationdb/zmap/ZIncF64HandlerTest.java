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
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ZIncF64HandlerTest extends BaseHandlerTest {

    @Test
    void shouldIncrementNewKey() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Increment a non-existent key
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", 10.5).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the value was set
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(10.5, actualMessage.value(), 0.0001);
        }
    }

    @Test
    void shouldIncrementExistingKey() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set initial value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", 10.0).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Increment existing key
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", 25.5).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the sum
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(35.5, actualMessage.value(), 0.0001);
        }
    }

    @Test
    void shouldDecrementWithNegativeValue() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set initial value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", 100.0).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Decrement with negative value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", -30.5).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the result
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(69.5, actualMessage.value(), 0.0001);
        }
    }

    @Test
    void shouldAccumulateMultipleIncrements() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Multiple increments
        double expectedTotal = 0.0;
        for (int i = 1; i <= 5; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", i * 1.5).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            expectedTotal += i * 1.5;
        }

        // Verify the accumulated total (1.5 + 3.0 + 4.5 + 6.0 + 7.5 = 22.5)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(expectedTotal, actualMessage.value(), 0.0001);
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
            cmd.zincf64("tx-counter", 42.42).encode(buf);

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
            cmd.zgetf64("tx-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(42.42, actualMessage.value(), 0.0001);
        }
    }

    @Test
    void shouldNormalizeNegativeZero() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set positive value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", 5.0).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Subtract to get -0.0
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", -5.0).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Verify normalized to 0.0 (not -0.0)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(0.0, actualMessage.value(), 0.0);
            // Ensure it's not negative zero
            assertEquals(Double.doubleToRawLongBits(0.0), Double.doubleToRawLongBits(actualMessage.value()));
        }
    }

    @Test
    void shouldHandleFractionalValues() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Add fractional values
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", 1.25).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", 2.75).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("counter", 0.125).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Verify the sum (1.25 + 2.75 + 0.125 = 4.125)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetf64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(4.125, actualMessage.value(), 0.0001);
        }
    }

    @Test
    void shouldRejectOverflow() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set a value near Double.MAX_VALUE
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("overflow-counter", Double.MAX_VALUE).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Try to add more, causing overflow to Infinity
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincf64("overflow-counter", Double.MAX_VALUE).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals("ERR Resulting value is not a finite IEEE-754 double (overflow or invalid operation)", actualMessage.content());
        }
    }

    @Test
    void shouldHandleConcurrentIncrementsFromVirtualThreads() throws Exception {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Expected total: 1.0 + 2.0 + 3.0 + 4.0 + 5.0 + 6.0 + 7.0 + 8.0 + 9.0 + 10.0 = 55.0
        double expectedTotal = 55.0;

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();

            // Launch 10 virtual threads, each incrementing with values 1.0-10.0
            for (int i = 1; i <= 10; i++) {
                final double value = i;
                futures.add(executor.submit(() -> {
                    ByteBuf buf = Unpooled.buffer();
                    cmd.zincf64("concurrent-counter", value).encode(buf);

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
            cmd.zgetf64("concurrent-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(DoubleRedisMessage.class, response);
            DoubleRedisMessage actualMessage = (DoubleRedisMessage) response;
            assertEquals(expectedTotal, actualMessage.value(), 0.0001);
        }
    }
}
