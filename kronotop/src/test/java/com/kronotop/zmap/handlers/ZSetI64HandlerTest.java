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
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ZSetI64HandlerTest extends BaseHandlerTest {

    static Stream<Arguments> invalidArguments() {
        return Stream.of(
                arguments("too few arguments",
                        List.of("key"),
                        "ERR wrong number of arguments for 'ZSET.I64' command"),
                arguments("too many arguments",
                        List.of("key", "1", "NAMESPACE", "ns", "EXTRA"),
                        "ERR wrong number of arguments for 'ZSET.I64' command"),
                arguments("unknown keyword",
                        List.of("key", "1", "EXTRA"),
                        "ERR Unknown 'EXTRA' argument"),
                arguments("NAMESPACE without value",
                        List.of("key", "1", "NAMESPACE"),
                        "ERR NAMESPACE argument must be followed by a namespace"),
                arguments("value is not a number",
                        List.of("key", "abc"),
                        "ERR value is not a long or out of range")
        );
    }

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

    @Test
    void shouldSetKeyInGivenNamespace() {
        // Behavior: NAMESPACE writes the key in the given namespace and leaves the session's
        // current namespace unchanged.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        createNamespace(channel, namespace);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zseti64("key", 42, namespace).encode(buf);
            assertOK(runCommand(channel, buf));
        }
        {
            // The session still points at the default namespace, where the key does not exist.
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, response);
        }
        useNamespace(channel, namespace);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zgeti64("key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(42, ((IntegerRedisMessage) response).value());
        }
    }

    @Test
    void shouldRejectSetWhenNamespaceDoesNotExist() {
        // Behavior: NAMESPACE with an unknown namespace returns NOSUCHNAMESPACE.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.zseti64("key", 42, namespace).encode(buf);
        Object response = runCommand(getChannel(), buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(String.format("NOSUCHNAMESPACE No such namespace: '%s'", namespace),
                ((ErrorRedisMessage) response).content());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidArguments")
    void shouldRejectInvalidArguments(String name, List<String> rawArgs, String expectedError) {
        // Behavior: ZSET.I64 rejects a wrong argument count, an unknown keyword, a keyword without its
        // value or a value that is not a number with an exact ERR reply.
        Object response = runRaw(getChannel(), CommandType.ZSET_I64, rawArgs);

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(expectedError, ((ErrorRedisMessage) response).content());
    }
}
