package com.kronotop.foundationdb.zmap;

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

import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.protocol.NamespaceArgs;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ZDelPrefixHandlerTest extends BaseZMapTest {
    @Test
    public void testZDELRANGE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        String prefix = "my-prefix";

        {
            ByteBuf buf = Unpooled.buffer();
            NamespaceArgs args = NamespaceArgs.Builder.layer("layer".getBytes()).prefix(prefix.getBytes());
            cmd.namespaceCreate(namespace, args).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceOpen(namespace).encode(buf);
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

        // ZDELPREFIX
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zdelprefix(prefix.getBytes()).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        // ZGET - nil
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zget(namespace, String.format("key-%d", i)).encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();
                assertInstanceOf(FullBulkStringRedisMessage.class, response);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
                assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
            }
        }
    }
}
