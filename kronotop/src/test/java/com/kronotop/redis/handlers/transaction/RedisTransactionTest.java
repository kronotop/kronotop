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

package com.kronotop.redis.handlers.transaction;

import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.handlers.BaseRedisHandlerTest;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class RedisTransactionTest extends BaseRedisHandlerTest {
    @Test
    public void testTransaction_MULTI_EXEC() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-2", "value-2").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-2").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.exec().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

            RedisMessage firstMessage = actualMessage.children().get(0);
            assertInstanceOf(SimpleStringRedisMessage.class, firstMessage);
            SimpleStringRedisMessage actualFirstMessage = (SimpleStringRedisMessage) firstMessage;
            assertEquals(Response.OK, actualFirstMessage.content());

            RedisMessage secondMessage = actualMessage.children().get(1);
            assertInstanceOf(SimpleStringRedisMessage.class, secondMessage);
            SimpleStringRedisMessage actualSecondMessage = (SimpleStringRedisMessage) secondMessage;
            assertEquals(Response.OK, actualSecondMessage.content());


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

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.discard().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());

            List<Request> queuedCommands = channel.attr(SessionAttributes.QUEUED_COMMANDS).get();
            assertEquals(0, queuedCommands.size());
            assertFalse(channel.attr(SessionAttributes.MULTI).get());
            assertFalse(channel.attr(SessionAttributes.MULTI_DISCARDED).get());
            assertNull(channel.attr(SessionAttributes.WATCHED_KEYS).get());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-1").encode(buf);

            Object msg = runCommand(channel, buf);
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

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            Object msg = runCommand(channel, buf);
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

            Object msg = runCommand(channel, buf);
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

            Object msg = runCommand(channel, buf);
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

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.exec().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

            RedisMessage firstMessage = actualMessage.children().get(0);
            assertInstanceOf(SimpleStringRedisMessage.class, firstMessage);
            SimpleStringRedisMessage actualFirstMessage = (SimpleStringRedisMessage) firstMessage;
            assertEquals(Response.OK, actualFirstMessage.content());

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

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.multi().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("key-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        // Modify a key, the ongoing transaction will be aborted
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            EmbeddedChannel secondChannel = newChannel();
            Object msg = runCommand(secondChannel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.exec().encode(buf);

            Object msg = runCommand(channel, buf);
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

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.watch("key-1").encode(buf);

            Object msg = runCommand(channel, buf);
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

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-1", "value-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("QUEUED", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            channel.writeInbound(new SimpleStringRedisMessage("foobar"));

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR unknown command 'FOOBAR'", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.exec().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("EXECABORT Transaction discarded because of previous errors.", actualMessage.content());
        }
    }
}
