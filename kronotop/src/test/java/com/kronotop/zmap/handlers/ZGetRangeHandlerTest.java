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
import com.kronotop.commands.ZGetRangeArgs;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ZGetRangeHandlerTest extends BaseHandlerTest {

    @Test
    void shouldGetRange() {
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

        // ZGETRANGE key-0 key-5
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.begin("key-0".getBytes()).end("key-5".getBytes());
            cmd.zgetrange(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;

            int i = 0;
            for (RedisMessage redisMessage : actualMessage.children()) {
                ArrayRedisMessage item = (ArrayRedisMessage) redisMessage;

                RedisMessage rawKey = item.children().get(0);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawKey);
                FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) rawKey;
                assertEquals(String.format("key-%d", i), key.content().toString(StandardCharsets.US_ASCII));

                RedisMessage rawValue = item.children().get(1);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawValue);
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) rawValue;
                assertEquals(String.format("value-%d", i), value.content().toString(StandardCharsets.US_ASCII));

                i++;
            }
        }

        // ZGETRANGE key-0 key-5 LIMIT 3
        {
            int expectedLimit = 3;
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.
                    begin("key-0".getBytes()).
                    end("key-5".getBytes()).
                    limit(expectedLimit);
            cmd.zgetrange(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;

            int i = 0;
            for (RedisMessage redisMessage : actualMessage.children()) {
                ArrayRedisMessage item = (ArrayRedisMessage) redisMessage;

                RedisMessage rawKey = item.children().get(0);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawKey);
                FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) rawKey;
                assertEquals(String.format("key-%d", i), key.content().toString(StandardCharsets.US_ASCII));

                RedisMessage rawValue = item.children().get(1);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawValue);
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) rawValue;
                assertEquals(String.format("value-%d", i), value.content().toString(StandardCharsets.US_ASCII));

                i++;
            }
            assertEquals(expectedLimit, i);
        }

        // ZGETRANGE key-0 key-5 LIMIT 3 REVERSE
        {
            int expectedLimit = 3;
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.
                    begin("key-0".getBytes()).
                    end("key-5".getBytes()).
                    limit(expectedLimit).
                    reverse();
            cmd.zgetrange(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;

            int i = 5;
            for (RedisMessage redisMessage : actualMessage.children()) {
                ArrayRedisMessage item = (ArrayRedisMessage) redisMessage;

                RedisMessage rawKey = item.children().get(0);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawKey);
                FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) rawKey;
                assertEquals(String.format("key-%d", i), key.content().toString(StandardCharsets.US_ASCII));

                RedisMessage rawValue = item.children().get(1);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawValue);
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) rawValue;
                assertEquals(String.format("value-%d", i), value.content().toString(StandardCharsets.US_ASCII));

                i--;
            }
        }

        // ZGETRANGE * *
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.
                    begin("*".getBytes()).
                    end("*".getBytes());
            cmd.zgetrange(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;

            int i = 0;
            for (RedisMessage redisMessage : actualMessage.children()) {
                ArrayRedisMessage item = (ArrayRedisMessage) redisMessage;

                RedisMessage rawKey = item.children().get(0);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawKey);
                FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) rawKey;
                assertEquals(String.format("key-%d", i), key.content().toString(StandardCharsets.US_ASCII));

                RedisMessage rawValue = item.children().get(1);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawValue);
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) rawValue;
                assertEquals(String.format("value-%d", i), value.content().toString(StandardCharsets.US_ASCII));

                i++;
            }
            assertEquals(10, i);
        }

        // ZDELRANGE <namespace> key-2 *
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.
                    begin("key-2".getBytes()).
                    end("*".getBytes());
            cmd.zgetrange(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;

            int i = 2;
            for (RedisMessage redisMessage : actualMessage.children()) {
                ArrayRedisMessage item = (ArrayRedisMessage) redisMessage;

                RedisMessage rawKey = item.children().get(0);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawKey);
                FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) rawKey;
                assertEquals(String.format("key-%d", i), key.content().toString(StandardCharsets.US_ASCII));

                RedisMessage rawValue = item.children().get(1);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawValue);
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) rawValue;
                assertEquals(String.format("value-%d", i), value.content().toString(StandardCharsets.US_ASCII));

                i++;
            }
            assertEquals(10, i);
        }

        // ZGETRANGE key-2 * begin_key_selector first_greater_than
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.
                    begin("key-2".getBytes()).
                    end("*".getBytes()).
                    beginKeySelector("first_greater_than");
            cmd.zgetrange(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;

            int i = 3;
            for (RedisMessage redisMessage : actualMessage.children()) {
                ArrayRedisMessage item = (ArrayRedisMessage) redisMessage;

                RedisMessage rawKey = item.children().get(0);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawKey);
                FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) rawKey;
                assertEquals(String.format("key-%d", i), key.content().toString(StandardCharsets.US_ASCII));

                RedisMessage rawValue = item.children().get(1);
                assertInstanceOf(FullBulkStringRedisMessage.class, rawValue);
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) rawValue;
                assertEquals(String.format("value-%d", i), value.content().toString(StandardCharsets.US_ASCII));

                i++;
            }
            assertEquals(10, i);
        }
    }

    @Test
    void shouldGetRangeWithWildcardBeginAndExplicitEnd() {
        // Behavior: ZGETRANGE * key-5 returns keys from subspace start through key-5 (inclusive, default end selector is FIRST_GREATER_THAN)
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        for (int i = 0; i < 10; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset(String.format("key-%d", i), String.format("value-%d", i)).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        ByteBuf buf = Unpooled.buffer();
        ZGetRangeArgs args = ZGetRangeArgs.Builder.
                begin("*".getBytes()).
                end("key-5".getBytes());
        cmd.zgetrange(args).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;

        int i = 0;
        for (RedisMessage redisMessage : actualMessage.children()) {
            ArrayRedisMessage item = (ArrayRedisMessage) redisMessage;

            RedisMessage rawKey = item.children().get(0);
            assertInstanceOf(FullBulkStringRedisMessage.class, rawKey);
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) rawKey;
            assertEquals(String.format("key-%d", i), key.content().toString(StandardCharsets.US_ASCII));

            RedisMessage rawValue = item.children().get(1);
            assertInstanceOf(FullBulkStringRedisMessage.class, rawValue);
            FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) rawValue;
            assertEquals(String.format("value-%d", i), value.content().toString(StandardCharsets.US_ASCII));

            i++;
        }
        assertEquals(6, i);
    }

    @Test
    void shouldGetRangeWithWildcardBeginAndExplicitEndReverse() {
        // Behavior: ZGETRANGE * key-5 REVERSE returns keys from key-5 (inclusive) down to subspace start
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        for (int i = 0; i < 10; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset(String.format("key-%d", i), String.format("value-%d", i)).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        ByteBuf buf = Unpooled.buffer();
        ZGetRangeArgs args = ZGetRangeArgs.Builder.
                begin("*".getBytes()).
                end("key-5".getBytes()).
                reverse();
        cmd.zgetrange(args).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;

        int i = 5;
        for (RedisMessage redisMessage : actualMessage.children()) {
            ArrayRedisMessage item = (ArrayRedisMessage) redisMessage;

            RedisMessage rawKey = item.children().get(0);
            assertInstanceOf(FullBulkStringRedisMessage.class, rawKey);
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) rawKey;
            assertEquals(String.format("key-%d", i), key.content().toString(StandardCharsets.US_ASCII));

            RedisMessage rawValue = item.children().get(1);
            assertInstanceOf(FullBulkStringRedisMessage.class, rawValue);
            FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) rawValue;
            assertEquals(String.format("value-%d", i), value.content().toString(StandardCharsets.US_ASCII));

            i--;
        }
        assertEquals(-1, i);
    }

    static Stream<Arguments> invalidArguments() {
        String limitValue = "ERR LIMIT argument must be followed by a positive integer";
        return Stream.of(
                arguments("unknown keyword",
                        List.of("key-0", "key-5", "LIMI", "3"),
                        "ERR Unknown 'LIMI' argument"),
                arguments("old underscore spelling",
                        List.of("key-0", "key-5", "BEGIN_KEY_SELECTOR", "first_greater_than"),
                        "ERR Unknown 'BEGIN_KEY_SELECTOR' argument"),
                arguments("limit without value",
                        List.of("key-0", "key-5", "LIMIT"),
                        limitValue),
                arguments("zero limit",
                        List.of("key-0", "key-5", "LIMIT", "0"),
                        limitValue),
                arguments("negative limit",
                        List.of("key-0", "key-5", "LIMIT", "-1"),
                        limitValue),
                arguments("non-numeric limit",
                        List.of("key-0", "key-5", "LIMIT", "abc"),
                        "ERR value is not a int or out of range"),
                arguments("begin selector without value",
                        List.of("key-0", "key-5", "BEGIN-KEY-SELECTOR"),
                        "ERR BEGIN-KEY-SELECTOR argument must be followed by a valid key selector"),
                arguments("end selector without value",
                        List.of("key-0", "key-5", "END-KEY-SELECTOR"),
                        "ERR END-KEY-SELECTOR argument must be followed by a valid key selector"),
                arguments("invalid key selector",
                        List.of("key-0", "key-5", "BEGIN-KEY-SELECTOR", "bogus"),
                        "ERR Unknown range key selector: 'bogus'"),
                arguments("duplicate limit",
                        List.of("key-0", "key-5", "LIMIT", "3", "LIMIT", "5"),
                        "ERR Duplicate 'LIMIT' argument"),
                arguments("duplicate reverse in mixed case",
                        List.of("key-0", "key-5", "REVERSE", "reverse"),
                        "ERR Duplicate 'REVERSE' argument"),
                arguments("duplicate begin key selector",
                        List.of("key-0", "key-5",
                                "BEGIN-KEY-SELECTOR", "first_greater_than",
                                "BEGIN-KEY-SELECTOR", "first_greater_or_equal"),
                        "ERR Duplicate 'BEGIN-KEY-SELECTOR' argument"),
                arguments("duplicate end key selector",
                        List.of("key-0", "key-5",
                                "END-KEY-SELECTOR", "first_greater_than",
                                "END-KEY-SELECTOR", "first_greater_or_equal"),
                        "ERR Duplicate 'END-KEY-SELECTOR' argument")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidArguments")
    void shouldRejectInvalidArguments(String name, List<String> rawArgs, String expectedError) {
        // Behavior: ZGETRANGE rejects malformed keyword arguments with an ERR reply instead of
        // silently ignoring them.
        Object response = runRaw(getChannel(), CommandType.ZGETRANGE, rawArgs);

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(expectedError, ((ErrorRedisMessage) response).content());
    }

    @Test
    void shouldReturnErrorWhenArgumentCountExceedsMaximum() {
        // Behavior: More than nine arguments is rejected before the handler runs.
        Object response = runRaw(getChannel(), CommandType.ZGETRANGE, List.of(
                "key-0", "key-5", "LIMIT", "3", "REVERSE",
                "BEGIN-KEY-SELECTOR", "first_greater_or_equal",
                "END-KEY-SELECTOR", "first_greater_than", "EXTRA"));

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertTrue(((ErrorRedisMessage) response).content().contains("wrong number of arguments"));
    }

    @Test
    void shouldReturnEmptyArrayForInvertedRange() {
        // Behavior: A begin key larger than the end key is not an error. The command returns an empty array.
        Object response = runRaw(getChannel(), CommandType.ZGETRANGE, List.of("key-5", "key-0"));

        assertInstanceOf(ArrayRedisMessage.class, response);
        assertEquals(0, ((ArrayRedisMessage) response).children().size());
    }
}
