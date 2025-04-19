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
import com.kronotop.protocol.ZMutateArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ZMutateHandlerTest extends BaseHandlerTest {

    @Test
    void test_ZMUTATE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset("my-key", "my-value").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zmutate("my-key", "my-value", ZMutateArgs.Builder.compareAndClear()).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zget("my-key").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
        }
    }

    @Test
    public void test_ZMUTATE_ADD_INTEGER() {
        KronotopCommandBuilder<byte[], byte[]> cmd = new KronotopCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();

        byte[] delta = new byte[]{1, 0, 0, 0, 0, 0, 0, 0};
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset("my-key".getBytes(), delta).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zmutate("my-key".getBytes(), delta, ZMutateArgs.Builder.add()).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zget("my-key".getBytes()).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            byte[] rawItem = new byte[actualMessage.content().readableBytes()];
            actualMessage.content().readBytes(rawItem);

            long result = ByteBuffer.wrap(rawItem).order(ByteOrder.LITTLE_ENDIAN).getLong();
            assertEquals(2, result);
        }
    }
}