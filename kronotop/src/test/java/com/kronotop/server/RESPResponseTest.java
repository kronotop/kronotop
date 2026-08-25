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
import java.nio.ByteBuffer;
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
    void shouldWriteDoubleAsDoubleTypeWhenRESP3() {
        // Behavior: writeDouble emits the RESP3 double type on a RESP3 session.
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeDouble(100);
        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(DoubleRedisMessage.class, redisMessage);
        DoubleRedisMessage doubleRedisMessage = (DoubleRedisMessage) redisMessage;
        assertEquals(100, doubleRedisMessage.value());
    }

    @Test
    void shouldWriteDoubleAsBulkStringWhenRESP2() {
        // Behavior: RESP2 has no double type, so writeDouble emits a bulk string on a RESP2 session.
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeDouble(100);

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        try {
            assertInstanceOf(FullBulkStringRedisMessage.class, redisMessage);
            FullBulkStringRedisMessage bulkString = (FullBulkStringRedisMessage) redisMessage;

            byte[] data = new byte[bulkString.content().readableBytes()];
            bulkString.content().readBytes(data);
            assertEquals(100, ByteBuffer.wrap(data).getDouble());
        } finally {
            ReferenceCountUtil.release(redisMessage);
        }
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

    @Test
    void shouldWriteMapAsFlatArrayWhenRESP2() {
        // Behavior: RESP2 has no map type, so writeMap emits a flat array of keys and values in the same order.
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(new SimpleStringRedisMessage("first"), new IntegerRedisMessage(1));
        map.put(new SimpleStringRedisMessage("second"), new IntegerRedisMessage(2));

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeMap(map);

        List<RedisMessage> children = flatArrayOf(ctx.embeddedChannel().readOutbound());
        assertEquals(4, children.size());
        assertEquals("first", ((SimpleStringRedisMessage) children.get(0)).content());
        assertEquals(1, ((IntegerRedisMessage) children.get(1)).value());
        assertEquals("second", ((SimpleStringRedisMessage) children.get(2)).content());
        assertEquals(2, ((IntegerRedisMessage) children.get(3)).value());
    }

    @Test
    void shouldFlattenNestedMapWhenRESP2() {
        // Behavior: a map nested under a field is flattened too, it is not left as a RESP3 map.
        Map<RedisMessage, RedisMessage> inner = new LinkedHashMap<>();
        inner.put(new SimpleStringRedisMessage("locale"), new SimpleStringRedisMessage("en"));

        Map<RedisMessage, RedisMessage> outer = new LinkedHashMap<>();
        outer.put(new SimpleStringRedisMessage("collation"), new MapRedisMessage(inner));

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeMap(outer);

        List<RedisMessage> children = flatArrayOf(ctx.embeddedChannel().readOutbound());
        assertEquals(2, children.size());

        List<RedisMessage> collation = flatArrayOf(children.get(1));
        assertEquals(2, collation.size());
        assertEquals("locale", ((SimpleStringRedisMessage) collation.get(0)).content());
        assertEquals("en", ((SimpleStringRedisMessage) collation.get(1)).content());
    }

    @Test
    void shouldFlattenMapInsideArrayWhenRESP2() {
        // Behavior: the outer array keeps its shape, the map it holds becomes a flat array.
        Map<RedisMessage, RedisMessage> segment = new LinkedHashMap<>();
        segment.put(new SimpleStringRedisMessage("id"), new IntegerRedisMessage(7));

        List<RedisMessage> array = new ArrayList<>();
        array.add(new MapRedisMessage(segment));

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeArray(array);

        List<RedisMessage> children = flatArrayOf(ctx.embeddedChannel().readOutbound());
        assertEquals(1, children.size());

        List<RedisMessage> first = flatArrayOf(children.get(0));
        assertEquals(2, first.size());
        assertEquals("id", ((SimpleStringRedisMessage) first.get(0)).content());
        assertEquals(7, ((IntegerRedisMessage) first.get(1)).value());
    }

    @Test
    void shouldFlattenThreeLevelTreeWhenRESP2() {
        // Behavior: the rewrite walks the whole tree, so a map under an array under a map is flattened.
        Map<RedisMessage, RedisMessage> leaf = new LinkedHashMap<>();
        leaf.put(new SimpleStringRedisMessage("segment_id"), new IntegerRedisMessage(3));

        List<RedisMessage> segments = new ArrayList<>();
        segments.add(new MapRedisMessage(leaf));

        Map<RedisMessage, RedisMessage> root = new LinkedHashMap<>();
        root.put(new SimpleStringRedisMessage("segments"), new ArrayRedisMessage(segments));

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeMap(root);

        List<RedisMessage> children = flatArrayOf(ctx.embeddedChannel().readOutbound());
        assertEquals(2, children.size());

        List<RedisMessage> segmentList = flatArrayOf(children.get(1));
        assertEquals(1, segmentList.size());

        List<RedisMessage> leafFields = flatArrayOf(segmentList.get(0));
        assertEquals(2, leafFields.size());
        assertEquals("segment_id", ((SimpleStringRedisMessage) leafFields.get(0)).content());
        assertEquals(3, ((IntegerRedisMessage) leafFields.get(1)).value());
    }

    @Test
    void shouldWriteBooleanAsIntegerWhenRESP2() {
        // Behavior: RESP2 has no boolean type, so writeBoolean emits 1 or 0.
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);

        response.writeBoolean(true);
        RedisMessage trueMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(IntegerRedisMessage.class, trueMessage);
        assertEquals(1, ((IntegerRedisMessage) trueMessage).value());

        response.writeBoolean(false);
        RedisMessage falseMessage = ctx.embeddedChannel().readOutbound();
        assertInstanceOf(IntegerRedisMessage.class, falseMessage);
        assertEquals(0, ((IntegerRedisMessage) falseMessage).value());
    }

    @Test
    void shouldWriteNestedBooleanAsIntegerWhenRESP2() {
        // Behavior: a boolean nested in a map is rewritten too, not only the one writeBoolean sends.
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(new SimpleStringRedisMessage("unique"), BooleanRedisMessage.TRUE);

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeMap(map);

        List<RedisMessage> children = flatArrayOf(ctx.embeddedChannel().readOutbound());
        assertInstanceOf(IntegerRedisMessage.class, children.get(1));
        assertEquals(1, ((IntegerRedisMessage) children.get(1)).value());
    }

    @Test
    void shouldWriteNestedDoubleAsBulkStringWhenRESP2() {
        // Behavior: a double nested in a map becomes the same 8 byte bulk string writeDouble sends.
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(new SimpleStringRedisMessage("fill_ratio"), new DoubleRedisMessage(0.75));

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeMap(map);

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        try {
            List<RedisMessage> children = flatArrayOf(redisMessage);
            assertEquals(0.75, doubleOf(children.get(1)));
        } finally {
            ReferenceCountUtil.release(redisMessage);
        }
    }

    @Test
    void shouldWriteNestedNullAsBulkStringWhenRESP2() {
        // Behavior: a null inside an array becomes the RESP2 null bulk string.
        List<RedisMessage> array = new ArrayList<>();
        array.add(NullRedisMessage.INSTANCE);

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeArray(array);

        List<RedisMessage> children = flatArrayOf(ctx.embeddedChannel().readOutbound());
        assertEquals(1, children.size());
        assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, children.get(0));
    }

    @Test
    void shouldWriteSetAsArrayWhenRESP2() {
        // Behavior: RESP2 has no set type, so writeSet emits an array.
        Set<RedisMessage> set = new LinkedHashSet<>();
        set.add(new SimpleStringRedisMessage("foobar"));

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeSet(set);

        List<RedisMessage> children = flatArrayOf(ctx.embeddedChannel().readOutbound());
        assertEquals(1, children.size());
        assertEquals("foobar", ((SimpleStringRedisMessage) children.get(0)).content());
    }

    @Test
    void shouldWriteBigNumberAsBulkStringWhenRESP2() {
        // Behavior: RESP2 has no big number type, so the value is sent as a bulk string.
        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeBigNumber("100");

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        try {
            assertEquals("100", stringOf(redisMessage));
        } finally {
            ReferenceCountUtil.release(redisMessage);
        }
    }

    @Test
    void shouldWriteVerbatimStringAsBulkStringWhenRESP2() {
        // Behavior: RESP2 has no verbatim string, so the format prefix is dropped and the rest is sent as a bulk string.
        ByteBuf content = Unpooled.buffer();
        content.writeBytes("txt:message".getBytes(StandardCharsets.UTF_8));

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeVerbatimString(content);

        RedisMessage redisMessage = ctx.embeddedChannel().readOutbound();
        try {
            assertFalse(redisMessage instanceof FullBulkVerbatimStringRedisMessage);
            assertEquals("message", stringOf(redisMessage));
        } finally {
            ReferenceCountUtil.release(redisMessage);
        }
    }

    @Test
    void shouldKeepArrayInstanceWhenRESP2() {
        // Behavior: an array with no RESP3 only type is passed through, no new array is allocated.
        List<RedisMessage> children = new ArrayList<>();
        children.add(new SimpleStringRedisMessage("first"));
        children.add(new IntegerRedisMessage(2));
        ArrayRedisMessage array = new ArrayRedisMessage(children);

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP2);
        response.writeRedisMessage(array);

        assertSame(array, ctx.embeddedChannel().readOutbound());
    }

    @Test
    void shouldKeepMessageUnchangedWhenRESP3() {
        // Behavior: a RESP3 session receives the message tree as it is, nothing is rewritten.
        Map<RedisMessage, RedisMessage> inner = new LinkedHashMap<>();
        inner.put(new SimpleStringRedisMessage("unique"), BooleanRedisMessage.TRUE);

        Map<RedisMessage, RedisMessage> root = new LinkedHashMap<>();
        root.put(new SimpleStringRedisMessage("index"), new MapRedisMessage(inner));
        MapRedisMessage message = new MapRedisMessage(root);

        RESPResponse response = new RESPResponse(ctx, RESPVersion.RESP3);
        response.writeRedisMessage(message);

        assertSame(message, ctx.embeddedChannel().readOutbound());
    }

    private List<RedisMessage> flatArrayOf(RedisMessage message) {
        assertInstanceOf(ArrayRedisMessage.class, message);
        return ((ArrayRedisMessage) message).children();
    }

    private double doubleOf(RedisMessage message) {
        assertInstanceOf(FullBulkStringRedisMessage.class, message);
        ByteBuf content = ((FullBulkStringRedisMessage) message).content();
        byte[] data = new byte[content.readableBytes()];
        content.readBytes(data);
        assertEquals(8, data.length);
        return ByteBuffer.wrap(data).getDouble();
    }

    private String stringOf(RedisMessage message) {
        assertInstanceOf(FullBulkStringRedisMessage.class, message);
        return ((FullBulkStringRedisMessage) message).content().toString(StandardCharsets.UTF_8);
    }
}
