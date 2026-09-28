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
import com.kronotop.commands.CommandType;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ZSetD128HandlerTest extends BaseHandlerTest {

    static Stream<Arguments> invalidArguments() {
        return Stream.of(
                arguments("too few arguments",
                        List.of("key"),
                        "ERR wrong number of arguments for 'ZSET.D128' command"),
                arguments("too many arguments",
                        List.of("key", "value", "NAMESPACE", "ns", "EXTRA"),
                        "ERR wrong number of arguments for 'ZSET.D128' command"),
                arguments("unknown keyword",
                        List.of("key", "value", "EXTRA"),
                        "ERR Unknown 'EXTRA' argument"),
                arguments("NAMESPACE without value",
                        List.of("key", "value", "NAMESPACE"),
                        "ERR NAMESPACE argument must be followed by a namespace")
        );
    }

    @Test
    void shouldSetAndGetValue() {
        // Behavior: ZSET.D128 stores a value that ZGET.D128 can read back.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetd128("key", "123.456").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            assertEquals("123.456", ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldOverwriteExistingValue() {
        // Behavior: A second ZSET.D128 overwrites the first value.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetd128("key", "1.0").encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetd128("key", "2.0").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            assertEquals("2.0", ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldSetWithinTransaction() {
        // Behavior: ZSET.D128 within BEGIN/COMMIT persists the value.
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
            cmd.zsetd128("tx-key", "42.42").encode(buf);
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
            cmd.zgetd128("tx-key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            assertEquals("42.42", ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldInteropWithZinc() {
        // Behavior: ZSET.D128 then ZINC.D128 produces the sum readable via ZGET.D128.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetd128("counter", "100.5").encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zincd128("counter", "25.5").encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("counter").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            assertEquals("126.0", ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldRejectInvalidDecimalString() {
        // Behavior: ZSET.D128 rejects non-numeric strings with an error.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        cmd.zsetd128("key", "not-a-number").encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals("ERR invalid decimal", ((ErrorRedisMessage) response).content());
    }

    @Test
    void shouldPreserveHighPrecision() {
        // Behavior: ZSET.D128 preserves high-precision decimal values.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetd128("key", "0.123456789012345678901234567890123").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            assertEquals("0.123456789012345678901234567890123",
                    ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldSetKeyInGivenNamespace() {
        // Behavior: NAMESPACE writes the key in the given namespace and leaves the session's
        // current namespace unchanged.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        createNamespace(channel, namespace);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zsetd128("key", "123.456", namespace).encode(buf);
            assertOK(runCommand(channel, buf));
        }
        {
            // The session still points at the default namespace, where the key does not exist.
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, response);
        }
        useNamespace(channel, namespace);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgetd128("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            assertEquals("123.456", ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldRejectSetWhenNamespaceDoesNotExist() {
        // Behavior: NAMESPACE with an unknown namespace returns NOSUCHNAMESPACE.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.zsetd128("key", "123.456", namespace).encode(buf);
        Object response = runCommand(getChannel(), buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(String.format("NOSUCHNAMESPACE No such namespace: '%s'", namespace),
                ((ErrorRedisMessage) response).content());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidArguments")
    void shouldRejectInvalidArguments(String name, List<String> rawArgs, String expectedError) {
        // Behavior: ZSET.D128 rejects a wrong argument count, an unknown keyword or a keyword without
        // its value with an exact ERR reply.
        Object response = runRaw(getChannel(), CommandType.ZSET_D128, rawArgs);

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(expectedError, ((ErrorRedisMessage) response).content());
    }
}
