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

package com.kronotop.server;

import com.kronotop.server.impl.RESPResponse;
import com.kronotop.server.resp3.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RESPResponseTest {

    private MockChannelHandlerContext ctx;

    @BeforeEach
    void setup() {
        EmbeddedChannel channel = new EmbeddedChannel();
        ctx = new MockChannelHandlerContext(channel);
    }

    @AfterEach
    void tearDown() {
        ctx.embeddedChannel().finishAndReleaseAll();
    }

    @Test
    void shouldWriteRedisMessage() {
        // Create a RespResponse object and associate it with the channel
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);

        // Call the writeOK() method to add a simple 'OK' string to the response
        response.writeRedisMessage(new SimpleStringRedisMessage("Hello!"));

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, redisMessage);
        SimpleStringRedisMessage simpleStringRedisMessage = (SimpleStringRedisMessage) redisMessage;
        assertEquals("Hello!", simpleStringRedisMessage.content());
    }

    @Test
    void shouldWriteOK() {
        // Create a RespResponse object and associate it with the channel
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);

        // Call the writeOK() method to add a simple 'OK' string to the response
        response.writeOK();

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, redisMessage);
        SimpleStringRedisMessage simpleStringRedisMessage = (SimpleStringRedisMessage) redisMessage;
        assertEquals(Response.OK, simpleStringRedisMessage.content());
    }

    @Disabled("This test is skipped because EmbeddedChannel.flush doesn't work as expected.")
    @Test
    void shouldWriteQUEUED() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeQUEUED();
        response.flush();

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, redisMessage);
        SimpleStringRedisMessage simpleStringRedisMessage = (SimpleStringRedisMessage) redisMessage;
        assertEquals("QUEUED", simpleStringRedisMessage.content());
    }

    @Test
    void shouldWriteInteger() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeInteger(100);
        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(IntegerRedisMessage.class, redisMessage);
        IntegerRedisMessage integerRedisMessage = (IntegerRedisMessage) redisMessage;
        assertEquals(100, integerRedisMessage.value());
    }

    @Test
    void shouldWriteDouble() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeDouble(100);
        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(DoubleRedisMessage.class, redisMessage);
        DoubleRedisMessage doubleRedisMessage = (DoubleRedisMessage) redisMessage;
        assertEquals(100, doubleRedisMessage.value());
    }

    @Test
    void shouldWriteArray() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        SimpleStringRedisMessage first = new SimpleStringRedisMessage("first message");
        DoubleRedisMessage second = new DoubleRedisMessage(100);
        List<RedisMessage> array = new ArrayList<>();
        array.add(first);
        array.add(second);

        response.writeArray(array);
        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, redisMessage);
        ArrayRedisMessage arrayRedisMessage = (ArrayRedisMessage) redisMessage;
        assertEquals(2, arrayRedisMessage.children().size());

        SimpleStringRedisMessage firstMessage = (SimpleStringRedisMessage) arrayRedisMessage.children().get(0);
        assertEquals(first.content(), firstMessage.content());

        DoubleRedisMessage secondMessage = (DoubleRedisMessage) arrayRedisMessage.children().get(1);
        assertEquals(secondMessage.value(), secondMessage.value());
    }

    @Test
    void shouldWriteMap() {
        SimpleStringRedisMessage key = new SimpleStringRedisMessage("key");
        SimpleStringRedisMessage value = new SimpleStringRedisMessage("value");
        Map<RedisMessage, RedisMessage> map = new HashMap<>();
        map.put(key, value);

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeMap(map);

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(MapRedisMessage.class, redisMessage);
        MapRedisMessage mapRedisMessage = (MapRedisMessage) redisMessage;
        assertEquals(1, mapRedisMessage.children().size());

        RedisMessage message = mapRedisMessage.children().get(key);
        SimpleStringRedisMessage receivedValue = (SimpleStringRedisMessage) message;
        assertEquals(receivedValue.content(), value.content());
    }

    @Test
    void shouldWriteBoolean() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        {
            response.writeBoolean(true);
            RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
            assertInstanceOf(BooleanRedisMessage.class, redisMessage);
            BooleanRedisMessage booleanRedisMessage = (BooleanRedisMessage) redisMessage;
            assertTrue(booleanRedisMessage.value());
        }

        {
            response.writeBoolean(false);
            RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
            assertInstanceOf(BooleanRedisMessage.class, redisMessage);
            BooleanRedisMessage booleanRedisMessage = (BooleanRedisMessage) redisMessage;
            assertFalse(booleanRedisMessage.value());
        }
    }

    @Test
    void shouldWriteNullAsNullTypeWhenRESP3() {
        // Behavior: writeNULL emits the RESP3 null type on a RESP3 session.
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeNULL();
        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(NullRedisMessage.class, redisMessage);
        assertEquals(NullRedisMessage.INSTANCE, redisMessage);
    }

    @Test
    void shouldWriteNullAsBulkStringWhenRESP2() {
        // Behavior: writeNULL emits the RESP2 null bulk string on a RESP2 session.
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeNULL();
        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(FullBulkStringRedisMessage.class, redisMessage);
        assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, redisMessage);
    }

    @Test
    void shouldWriteBigNumber() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        {
            response.writeBigNumber(BigInteger.valueOf(100));
            RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
            assertInstanceOf(BigNumberRedisMessage.class, redisMessage);
            BigNumberRedisMessage bigNumberRedisMessage = (BigNumberRedisMessage) redisMessage;
            assertEquals("100", bigNumberRedisMessage.value());
        }

        {
            response.writeBigNumber("100");
            RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
            assertInstanceOf(BigNumberRedisMessage.class, redisMessage);
            BigNumberRedisMessage bigNumberRedisMessage = (BigNumberRedisMessage) redisMessage;
            assertEquals("100", bigNumberRedisMessage.value());
        }

        {
            response.writeBigNumber("100".getBytes());
            RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
            assertInstanceOf(BigNumberRedisMessage.class, redisMessage);
            BigNumberRedisMessage bigNumberRedisMessage = (BigNumberRedisMessage) redisMessage;
            assertEquals("100", bigNumberRedisMessage.value());
        }
    }

    @Test
    void shouldWriteVerbatimString() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);

        ByteBuf content = Unpooled.buffer();
        content.writeBytes("message".getBytes(StandardCharsets.UTF_8));

        response.writeVerbatimString(content);

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        try {
            assertInstanceOf(FullBulkVerbatimStringRedisMessage.class, redisMessage);

            FullBulkVerbatimStringRedisMessage message =
                    (FullBulkVerbatimStringRedisMessage) redisMessage;

            assertEquals(
                    "message",
                    message.content().toString(StandardCharsets.UTF_8)
            );
        } finally {
            ReferenceCountUtil.release(redisMessage);
        }
    }


    @Test
    void shouldWriteError() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeError("error message");

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, redisMessage);
        ErrorRedisMessage errorRedisMessage = (ErrorRedisMessage) redisMessage;
        assertEquals("ERR error message", errorRedisMessage.content());
    }

    @Test
    void shouldWriteErrorWithPrefix() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeError(RESPError.CROSSSLOT, "error message");

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, redisMessage);
        ErrorRedisMessage errorRedisMessage = (ErrorRedisMessage) redisMessage;
        assertEquals("CROSSSLOT error message", errorRedisMessage.content());
    }

    @Test
    void shouldWriteBulkError() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeBulkError("error message");

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        try {
            assertInstanceOf(FullBulkErrorStringRedisMessage.class, redisMessage);
            FullBulkErrorStringRedisMessage errorRedisMessage = (FullBulkErrorStringRedisMessage) redisMessage;
            byte[] data = new byte[errorRedisMessage.content().readableBytes()];
            errorRedisMessage.content().readBytes(data);
            assertEquals("ERR error message", new String(data));
        } finally {
            ReferenceCountUtil.release(redisMessage);
        }
    }

    @Test
    void shouldWriteBulkErrorWithPrefix() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeBulkError(RESPError.CROSSSLOT, "error message");

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        try {
            assertInstanceOf(FullBulkErrorStringRedisMessage.class, redisMessage);
            FullBulkErrorStringRedisMessage errorRedisMessage = (FullBulkErrorStringRedisMessage) redisMessage;
            byte[] data = new byte[errorRedisMessage.content().readableBytes()];
            errorRedisMessage.content().readBytes(data);
            assertEquals("CROSSSLOT error message", new String(data));
        } finally {
            ReferenceCountUtil.release(redisMessage);
        }
    }

    @Test
    void shouldWriteSimpleString() {
        // Create a RespResponse object and associate it with the channel
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);

        response.writeSimpleString("message");

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, redisMessage);
        SimpleStringRedisMessage simpleStringRedisMessage = (SimpleStringRedisMessage) redisMessage;
        assertEquals("message", simpleStringRedisMessage.content());
    }

    @Test
    void shouldWriteFullBulkString() {
        // Create a RespResponse object and associate it with the channel
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);

        ByteBuf content = Unpooled.copiedBuffer("message", CharsetUtil.UTF_8);
        response.writeFullBulkString(new FullBulkStringRedisMessage(content));

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        try {
            assertInstanceOf(FullBulkStringRedisMessage.class, redisMessage);
            FullBulkStringRedisMessage fullBulkStringRedisMessage = (FullBulkStringRedisMessage) redisMessage;

            byte[] data = new byte[fullBulkStringRedisMessage.content().readableBytes()];
            fullBulkStringRedisMessage.content().readBytes(data);
            assertEquals("message", new String(data));
        } finally {
            ReferenceCountUtil.release(redisMessage);
        }
    }

    @Test
    void shouldWrite() {
        // Create a RespResponse object and associate it with the channel
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);

        ByteBuf content = Unpooled.copiedBuffer("message", CharsetUtil.UTF_8);
        response.write(content);

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        try {
            assertInstanceOf(FullBulkStringRedisMessage.class, redisMessage);
            FullBulkStringRedisMessage fullBulkStringRedisMessage = (FullBulkStringRedisMessage) redisMessage;

            byte[] data = new byte[fullBulkStringRedisMessage.content().readableBytes()];
            fullBulkStringRedisMessage.content().readBytes(data);
            assertEquals("message", new String(data));
        } finally {
            ReferenceCountUtil.release(redisMessage);
        }
    }

    @Test
    void shouldWriteSet() {
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);

        Set<RedisMessage> set = new HashSet<>();
        set.add(new SimpleStringRedisMessage("foobar"));
        response.writeSet(set);

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(SetRedisMessage.class, redisMessage);
        SetRedisMessage setRedisMessage = (SetRedisMessage) redisMessage;
        SimpleStringRedisMessage message = (SimpleStringRedisMessage) setRedisMessage.children().iterator().next();
        assertEquals("foobar", message.content());
    }
}