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

package com.kronotop.core.handlers.transaction;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.SnapshotReadArgs;
import com.kronotop.server.Response;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.Attribute;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotReadHandlerTest extends BaseHandlerTest {
    @Test
    void shouldEnableSnapshotRead() {
        EmbeddedChannel channel = getChannel();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            Attribute<Boolean> snapshotReadAttr = channel.attr(SessionAttributes.SNAPSHOT_READ);
            assertTrue(snapshotReadAttr.get());
        }
    }

    @Test
    void shouldEnableSnapshotReadWithLowercaseArgument() {
        // Behavior: SNAPSHOTREAD accepts case-insensitive arguments, lowercase "on" enables snapshot read
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$12\r\nSNAPSHOTREAD\r\n$2\r\non\r\n".getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, actualMessage.content());

        Attribute<Boolean> snapshotReadAttr = channel.attr(SessionAttributes.SNAPSHOT_READ);
        assertTrue(snapshotReadAttr.get());
    }

    @Test
    void shouldDisableSnapshotReadWithLowercaseArgument() {
        // Behavior: SNAPSHOTREAD accepts case-insensitive arguments, lowercase "off" disables snapshot read
        EmbeddedChannel channel = getChannel();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            Attribute<Boolean> snapshotReadAttr = channel.attr(SessionAttributes.SNAPSHOT_READ);
            assertTrue(snapshotReadAttr.get());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            buf.writeBytes("*2\r\n$12\r\nSNAPSHOTREAD\r\n$3\r\noff\r\n".getBytes(StandardCharsets.US_ASCII));

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            Attribute<Boolean> snapshotReadAttr = channel.attr(SessionAttributes.SNAPSHOT_READ);
            assertNull(snapshotReadAttr.get());
        }
    }

    @Test
    void shouldDisableSnapshotRead() {
        EmbeddedChannel channel = getChannel();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            Attribute<Boolean> snapshotReadAttr = channel.attr(SessionAttributes.SNAPSHOT_READ);
            assertTrue(snapshotReadAttr.get());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.snapshotRead(SnapshotReadArgs.Builder.off()).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            Attribute<Boolean> snapshotReadAttr = channel.attr(SessionAttributes.SNAPSHOT_READ);
            assertNull(snapshotReadAttr.get());
        }
    }
}
