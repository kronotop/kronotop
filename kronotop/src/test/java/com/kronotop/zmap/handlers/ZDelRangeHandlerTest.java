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
import com.kronotop.commands.ZDelRangeArgs;
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

class ZDelRangeHandlerTest extends BaseHandlerTest {
    @Test
    void shouldDeleteKeyRange() {
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // ZSET
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zset(String.format("key-%d", i), String.format("value-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(SimpleStringRedisMessage.class, response);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
                assertEquals(Response.OK, actualMessage.content());
            }
        }

        // ZDELRANGE key-0 key-5
        {
            ByteBuf buf = Unpooled.buffer();
            ZDelRangeArgs args = ZDelRangeArgs.Builder.begin("key-0".getBytes()).end("key-5".getBytes());
            cmd.zdelrange(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // ZGET - nil
        {
            for (int i = 0; i < 5; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(String.format("key-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
            }
        }

        // ZGET
        {
            for (int i = 5; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(String.format("key-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(String.format("value-%d", i), actualMessage.content().toString(StandardCharsets.US_ASCII));
            }
        }
    }

    @Test
    void shouldDeleteRangeWithAsteriskBegin() {
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // ZSET
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zset(String.format("key-%d", i), String.format("value-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(SimpleStringRedisMessage.class, response);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
                assertEquals(Response.OK, actualMessage.content());
            }
        }

        // ZDELRANGE * key-5
        {
            ByteBuf buf = Unpooled.buffer();
            ZDelRangeArgs args = ZDelRangeArgs.Builder.begin("*".getBytes()).end("key-5".getBytes());
            cmd.zdelrange(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // ZGET - nil
        {
            for (int i = 0; i < 5; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(String.format("key-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
            }
        }

        // ZGET
        {
            for (int i = 5; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(String.format("key-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(String.format("value-%d", i), actualMessage.content().toString(StandardCharsets.US_ASCII));
            }
        }
    }

    @Test
    void shouldDeleteRangeWithAsteriskEnd() {
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // ZSET
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zset(String.format("key-%d", i), String.format("value-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(SimpleStringRedisMessage.class, response);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
                assertEquals(Response.OK, actualMessage.content());
            }
        }

        // ZDELRANGE key-5 *
        {
            ByteBuf buf = Unpooled.buffer();
            ZDelRangeArgs args = ZDelRangeArgs.Builder.begin("key-5".getBytes()).end("*".getBytes());
            cmd.zdelrange(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // ZGET - nil
        {
            for (int i = 5; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(String.format("key-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
            }
        }

        // ZGET
        {
            for (int i = 0; i < 5; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(String.format("key-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(String.format("value-%d", i), actualMessage.content().toString(StandardCharsets.US_ASCII));
            }
        }
    }

    static Stream<Arguments> invalidArguments() {
        return Stream.of(
                arguments("too few arguments",
                        List.of("key-0"),
                        "ERR wrong number of arguments for 'ZDELRANGE' command"),
                arguments("too many arguments",
                        List.of("key-0", "key-5", "EXTRA"),
                        "ERR wrong number of arguments for 'ZDELRANGE' command")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidArguments")
    void shouldRejectInvalidArguments(String name, List<String> rawArgs, String expectedError) {
        // Behavior: ZDELRANGE rejects a wrong argument count with an ERR reply.
        Object response = runRaw(getChannel(), CommandType.ZDELRANGE, rawArgs);

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(expectedError, ((ErrorRedisMessage) response).content());
    }

    @Test
    void shouldRejectInvertedRange() {
        // Behavior: A begin key larger than the end key fails with the INVERTED_RANGE error.
        Object response = runRaw(getChannel(), CommandType.ZDELRANGE, List.of("key-5", "key-0"));

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals("INVERTED_RANGE Range begin key larger than end key",
                ((ErrorRedisMessage) response).content());
    }

    @Test
    void shouldDeferInvertedRangeErrorToCommit() {
        // Behavior: Inside an explicit transaction the inverted range is accepted when the command
        // runs, and the error is reported at COMMIT.
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // BEGIN
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // ZDELRANGE key-5 key-0
        {
            Object response = runRaw(channel, CommandType.ZDELRANGE, List.of("key-5", "key-0"));
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // COMMIT
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            assertEquals("INVERTED_RANGE Range begin key larger than end key",
                    ((ErrorRedisMessage) response).content());
        }
    }
}
