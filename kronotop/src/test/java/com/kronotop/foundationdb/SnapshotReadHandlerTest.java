/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.foundationdb;

import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.protocol.SnapshotReadArgs;
import com.kronotop.server.ChannelAttributes;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.Attribute;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SnapshotReadHandlerTest extends BaseHandlerTest {
    @Test
    public void test_SNAPSHOT_READ_ON() {
        EmbeddedChannel channel = getChannel();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            Attribute<Boolean> snapshotReadAttr = channel.attr(ChannelAttributes.SNAPSHOT_READ);
            assertTrue(snapshotReadAttr.get());
        }
    }

    @Test
    public void test_SNAPSHOT_READ_OFF() {
        EmbeddedChannel channel = getChannel();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            Attribute<Boolean> snapshotReadAttr = channel.attr(ChannelAttributes.SNAPSHOT_READ);
            assertTrue(snapshotReadAttr.get());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.snapshotRead(SnapshotReadArgs.Builder.off()).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());

            Attribute<Boolean> snapshotReadAttr = channel.attr(ChannelAttributes.SNAPSHOT_READ);
            assertNull(snapshotReadAttr.get());
        }
    }
}
