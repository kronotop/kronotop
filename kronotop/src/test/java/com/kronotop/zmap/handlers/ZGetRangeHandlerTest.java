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
import com.kronotop.commands.ZGetRangeArgs;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

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
}
