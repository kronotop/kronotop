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
import com.kronotop.protocol.ZDelRangeArgs;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ZDelRangeHandlerTest extends BaseHandlerTest {
    @Test
    public void test_ZDELRANGE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // ZSET
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zset(String.format("key-%d", i), String.format("value-%d", i)).encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
                assertInstanceOf(SimpleStringRedisMessage.class, response);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
                assertEquals("OK", actualMessage.content());
            }
        }

        // ZDELRANGE key-0 key-5
        {
            ByteBuf buf = Unpooled.buffer();
            ZDelRangeArgs args = ZDelRangeArgs.Builder.begin("key-0".getBytes()).end("key-5".getBytes());
            cmd.zdelrange(args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        // ZGET - nil
        {
            for (int i = 0; i < 5; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(String.format("key-%d", i)).encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
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
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(String.format("value-%d", i), actualMessage.content().toString(StandardCharsets.US_ASCII));
            }
        }
    }

    @Test
    public void test_ZDELRANGE_begin_asterisk() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // ZSET
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zset(String.format("key-%d", i), String.format("value-%d", i)).encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
                assertInstanceOf(SimpleStringRedisMessage.class, response);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
                assertEquals("OK", actualMessage.content());
            }
        }

        // ZDELRANGE * key-5
        {
            ByteBuf buf = Unpooled.buffer();
            ZDelRangeArgs args = ZDelRangeArgs.Builder.begin("*".getBytes()).end("key-5".getBytes());
            cmd.zdelrange(args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        // ZGET - nil
        {
            for (int i = 0; i < 5; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(String.format("key-%d", i)).encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
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
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(String.format("value-%d", i), actualMessage.content().toString(StandardCharsets.US_ASCII));
            }
        }
    }

    @Test
    public void test_ZDELRANGE_end_asterisk() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // ZSET
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zset(String.format("key-%d", i), String.format("value-%d", i)).encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
                assertInstanceOf(SimpleStringRedisMessage.class, response);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
                assertEquals("OK", actualMessage.content());
            }
        }

        // ZDELRANGE key-5 *
        {
            ByteBuf buf = Unpooled.buffer();
            ZDelRangeArgs args = ZDelRangeArgs.Builder.begin("key-5".getBytes()).end("*".getBytes());
            cmd.zdelrange(args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        // ZGET - nil
        {
            for (int i = 5; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(String.format("key-%d", i)).encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
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
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(String.format("value-%d", i), actualMessage.content().toString(StandardCharsets.US_ASCII));
            }
        }
    }
}
