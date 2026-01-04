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
import com.kronotop.server.resp3.IntegerRedisMessage;
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

class ZIncI64HandlerTest extends BaseHandlerTest {

    @Test
    void shouldAccumulateMultipleIncrements() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Multiple increments
        long expectedTotal = 0;
        for (int i = 1; i <= 5; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.zinci64("counter", i * 10).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            expectedTotal += i * 10;
        }

        // Verify the accumulated total (10 + 20 + 30 + 40 + 50 = 150)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(expectedTotal, actualMessage.value());
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
            cmd.zinci64("tx-counter", 42).encode(buf);

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
            cmd.zgeti64("tx-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(42, actualMessage.value());
        }
    }

    @Test
    void shouldDecrementWithNegativeValue() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set initial value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zinci64("counter", 100).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Decrement with negative value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zinci64("counter", -30).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the result
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(70, actualMessage.value());
        }
    }

    @Test
    void shouldIncrementExistingKey() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set initial value
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zinci64("counter", 10).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Increment existing key
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zinci64("counter", 25).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the sum
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(35, actualMessage.value());
        }
    }

    @Test
    void shouldIncrementNewKey() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Increment a non-existent key
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zinci64("counter", 10).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify the value was set
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(10, actualMessage.value());
        }
    }

    @Test
    void shouldWrapAroundOnOverflow() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Set value to Long.MAX_VALUE
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zinci64("overflow-counter", Long.MAX_VALUE).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Add 1 to cause overflow (wraps to Long.MIN_VALUE)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zinci64("overflow-counter", 1).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify wrap-around to Long.MIN_VALUE (two's complement overflow)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("overflow-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(Long.MIN_VALUE, actualMessage.value());
        }
    }

    @Test
    void shouldHandleConcurrentIncrementsFromVirtualThreads() throws Exception {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Expected total: 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 = 55
        long expectedTotal = 55;

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();

            // Launch 10 virtual threads, each incrementing with values 1-10
            for (int i = 1; i <= 10; i++) {
                final long value = i;
                futures.add(executor.submit(() -> {
                    ByteBuf buf = Unpooled.buffer();
                    cmd.zinci64("concurrent-counter", value).encode(buf);

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
            cmd.zgeti64("concurrent-counter").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(expectedTotal, actualMessage.value());
        }
    }
}
