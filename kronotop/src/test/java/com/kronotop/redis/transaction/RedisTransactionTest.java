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

package com.kronotop.redis.transaction;

import com.kronotop.redis.BaseHandlerTest;
import com.kronotop.redistest.RedisCommandBuilder;
import com.kronotop.server.resp.ChannelAttributes;
import com.kronotop.server.resp.Request;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.*;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class RedisTransactionTest extends BaseHandlerTest {
    @Test
    public void testTransaction_MULTI_EXEC() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-2", "value-2").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-2").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.exec().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

            RedisMessage firstMessage = actualMessage.children().get(0);
            assertInstanceOf(SimpleStringRedisMessage.class, firstMessage);
            SimpleStringRedisMessage actualFirstMessage = (SimpleStringRedisMessage) firstMessage;
            assertEquals("OK", actualFirstMessage.content());

            RedisMessage secondMessage = actualMessage.children().get(1);
            assertInstanceOf(SimpleStringRedisMessage.class, secondMessage);
            SimpleStringRedisMessage actualSecondMessage = (SimpleStringRedisMessage) secondMessage;
            assertEquals("OK", actualSecondMessage.content());


            RedisMessage thirdMessage = actualMessage.children().get(2);
            assertInstanceOf(FullBulkStringRedisMessage.class, thirdMessage);
            FullBulkStringRedisMessage actualThirdMessage = (FullBulkStringRedisMessage) thirdMessage;
            assertEquals("value-1", actualThirdMessage.content().toString(CharsetUtil.US_ASCII));

            RedisMessage fourthMessage = actualMessage.children().get(3);
            assertInstanceOf(FullBulkStringRedisMessage.class, fourthMessage);
            FullBulkStringRedisMessage actualFourthMessage = (FullBulkStringRedisMessage) fourthMessage;
            assertEquals("value-2", actualFourthMessage.content().toString(CharsetUtil.US_ASCII));
        }
    }

    @Test
    public void testTransaction_DISCARD() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.discard().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());

            List<Request> queuedCommands = channel.attr(ChannelAttributes.QUEUED_COMMANDS).get();
            assertEquals(0, queuedCommands.size());
            assertFalse(channel.attr(ChannelAttributes.REDIS_MULTI).get());
            assertFalse(channel.attr(ChannelAttributes.REDIS_MULTI_DISCARDED).get());
            assertNull(channel.attr(ChannelAttributes.WATCHED_KEYS).get());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(FullBulkStringRedisMessage.class, msg);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
        }
    }

    @Test
    public void testTransaction_Disallow_Nested_MULTI() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR MULTI calls can not be nested", actualMessage.content());
        }
    }

    @Test
    public void testTransaction_DISCARD_Without_MULTI() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.discard().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR DISCARD without MULTI", actualMessage.content());
        }
    }

    @Test
    public void testTransaction_EXEC_Without_MULTI() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.exec().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR EXEC without MULTI", actualMessage.content());
        }
    }

    @Test
    public void testTransaction_WATCH() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.watch("key-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.exec().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

            RedisMessage firstMessage = actualMessage.children().get(0);
            assertInstanceOf(SimpleStringRedisMessage.class, firstMessage);
            SimpleStringRedisMessage actualFirstMessage = (SimpleStringRedisMessage) firstMessage;
            assertEquals("OK", actualFirstMessage.content());

            RedisMessage secondMessage = actualMessage.children().get(1);
            assertInstanceOf(FullBulkStringRedisMessage.class, secondMessage);
            FullBulkStringRedisMessage actualThirdMessage = (FullBulkStringRedisMessage) secondMessage;
            assertEquals("value-1", actualThirdMessage.content().toString(CharsetUtil.US_ASCII));
        }
    }

    @Test
    public void testTransaction_WATCH_Abort_Transaction() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.watch("key-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        // Modify a key, the ongoing transaction will be aborted
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            EmbeddedChannel secondChannel = newChannel();
            secondChannel.writeInbound(buf);
            Object msg = secondChannel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.exec().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(FullBulkStringRedisMessage.class, msg);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
            // Aborted
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
        }
    }

    @Test
    public void testTransaction_WATCH_Inside_MULTI_Is_Not_Allowed() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.watch("key-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR WATCH inside MULTI is not allowed", actualMessage.content());
        }
    }

    @Test
    public void testTransaction_EXECABORT() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            channel.writeInbound(new SimpleStringRedisMessage("foobar"));

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR unknown command 'FOOBAR'", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.exec().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("EXECABORT Transaction discarded because of previous errors.", actualMessage.content());
        }
    }
}
