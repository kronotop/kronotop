/*
 * Copyright (c) 2023 Kronotop
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

import com.kronotop.foundationdb.BaseHandlerTest;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.protocol.ZGetRangeArgs;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ZGetRangeHandlerTest extends BaseHandlerTest {
    @Test
    public void testZGETRANGE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreateOrOpen(namespace, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        // ZPUT
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zput(namespace, String.format("key-%d", i), String.format("value-%d", i)).encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
                assertInstanceOf(SimpleStringRedisMessage.class, response);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
                assertEquals("OK", actualMessage.content());
            }
        }

        // ZDELRANGE <namespace> key-0 key-5
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.begin("key-0".getBytes()).end("key-5".getBytes());
            cmd.zgetrange(namespace, args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
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

        // ZDELRANGE <namespace> key-0 key-5 LIMIT 3
        {
            int expectedLimit = 3;
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.
                    begin("key-0".getBytes()).
                    end("key-5".getBytes()).
                    limit(expectedLimit);
            cmd.zgetrange(namespace, args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
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

        // ZDELRANGE <namespace> key-0 key-5 LIMIT 3 REVERSE
        {
            int expectedLimit = 3;
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.
                    begin("key-0".getBytes()).
                    end("key-5".getBytes()).
                    limit(expectedLimit).
                    reverse();
            cmd.zgetrange(namespace, args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;

            int i = 4;
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

        // ZDELRANGE <namespace> * *
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.
                    begin("*".getBytes()).
                    end("*".getBytes());
            cmd.zgetrange(namespace, args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
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
            cmd.zgetrange(namespace, args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
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

        // ZDELRANGE <namespace> key-2 * begin_key_selector first_greater_than
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeArgs args = ZGetRangeArgs.Builder.
                    begin("key-2".getBytes()).
                    end("*".getBytes()).
                    beginKeySelector("first_greater_than");
            cmd.zgetrange(namespace, args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
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
}
