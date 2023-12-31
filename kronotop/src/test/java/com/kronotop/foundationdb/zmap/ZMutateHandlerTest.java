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
import com.kronotop.protocol.ZMutateArgs;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ZMutateHandlerTest extends BaseHandlerTest {
    @Test
    public void testZMutate() {
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

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zput(namespace, "my-key", "my-value").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zmutate(namespace, "my-key", "my-value", ZMutateArgs.Builder.compareAndClear()).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zget(namespace, "my-key").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
        }
    }

    @Test
    public void testZMUTATE_NAMESPACENOTOPEN() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        ZMutateArgs zMutateArgs = ZMutateArgs.Builder.max();
        cmd.zmutate(namespace, "my-key", "my-value", zMutateArgs).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals(String.format("NAMESPACENOTOPEN namespace '%s' not open", namespace), actualMessage.content());
    }
}