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

package com.kronotop.zmap.handlers;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.CommandType;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.commands.ZMutateArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

class ZMutateHandlerTest extends BaseHandlerTest {

    private byte[] longToLE(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    private long leToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private void zset(EmbeddedChannel channel, ZMapCommandBuilder<byte[], byte[]> cmd, byte[] key, byte[] value) {
        ByteBuf buf = Unpooled.buffer();
        cmd.zset(key, value).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private void zmutate(EmbeddedChannel channel, ZMapCommandBuilder<byte[], byte[]> cmd, byte[] key, byte[] param, ZMutateArgs args) {
        ByteBuf buf = Unpooled.buffer();
        cmd.zmutate(key, param, args).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private byte[] zget(EmbeddedChannel channel, ZMapCommandBuilder<byte[], byte[]> cmd, byte[] key) {
        ByteBuf buf = Unpooled.buffer();
        cmd.zget(key).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, response);
        FullBulkStringRedisMessage msg = (FullBulkStringRedisMessage) response;
        byte[] result = new byte[msg.content().readableBytes()];
        msg.content().readBytes(result);
        return result;
    }

    // --- ADD ---

    @Test
    void shouldMutateWithAddInteger() {
        // Behavior: ADD interprets existing value and param as little-endian integers and adds them.
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "add-key".getBytes();

        zset(channel, cmd, key, longToLE(1));
        zmutate(channel, cmd, key, longToLE(1), ZMutateArgs.Builder.add());

        assertEquals(2, leToLong(zget(channel, cmd, key)));
    }

    // --- BIT_AND ---

    @Test
    void shouldMutateWithBitAnd() {
        // Behavior: BIT_AND performs bitwise AND of the existing value and param.
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "bitand-key".getBytes();

        byte[] initial = new byte[]{(byte) 0xFF, 0x0F, 0, 0, 0, 0, 0, 0};
        byte[] param = new byte[]{0x0F, (byte) 0xFF, 0, 0, 0, 0, 0, 0};
        byte[] expected = new byte[]{0x0F, 0x0F, 0, 0, 0, 0, 0, 0};

        zset(channel, cmd, key, initial);
        zmutate(channel, cmd, key, param, ZMutateArgs.Builder.bitAnd());

        assertArrayEquals(expected, zget(channel, cmd, key));
    }

    // --- BIT_OR ---

    @Test
    void shouldMutateWithBitOr() {
        // Behavior: BIT_OR performs bitwise OR of the existing value and param.
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "bitor-key".getBytes();

        byte[] initial = new byte[]{(byte) 0xF0, 0x00, 0, 0, 0, 0, 0, 0};
        byte[] param = new byte[]{0x0F, (byte) 0xFF, 0, 0, 0, 0, 0, 0};
        byte[] expected = new byte[]{(byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0, 0, 0};

        zset(channel, cmd, key, initial);
        zmutate(channel, cmd, key, param, ZMutateArgs.Builder.bitOr());

        assertArrayEquals(expected, zget(channel, cmd, key));
    }

    // --- BIT_XOR ---

    @Test
    void shouldMutateWithBitXor() {
        // Behavior: BIT_XOR performs bitwise XOR of the existing value and param.
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "bitxor-key".getBytes();

        byte[] initial = new byte[]{(byte) 0xFF, 0x0F, 0, 0, 0, 0, 0, 0};
        byte[] param = new byte[]{(byte) 0xFF, (byte) 0xF0, 0, 0, 0, 0, 0, 0};
        byte[] expected = new byte[]{0x00, (byte) 0xFF, 0, 0, 0, 0, 0, 0};

        zset(channel, cmd, key, initial);
        zmutate(channel, cmd, key, param, ZMutateArgs.Builder.bitXor());

        assertArrayEquals(expected, zget(channel, cmd, key));
    }

    // --- MAX ---

    @Test
    void shouldMutateWithMax_paramIsLarger() {
        // Behavior: MAX stores the larger of the existing value and param (unsigned lexicographic comparison).
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "max-key-1".getBytes();

        zset(channel, cmd, key, longToLE(10));
        zmutate(channel, cmd, key, longToLE(20), ZMutateArgs.Builder.max());

        assertEquals(20, leToLong(zget(channel, cmd, key)));
    }

    @Test
    void shouldMutateWithMax_existingIsLarger() {
        // Behavior: MAX keeps the existing value when it is already the larger one.
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "max-key-2".getBytes();

        zset(channel, cmd, key, longToLE(20));
        zmutate(channel, cmd, key, longToLE(10), ZMutateArgs.Builder.max());

        assertEquals(20, leToLong(zget(channel, cmd, key)));
    }

    // --- MIN ---

    @Test
    void shouldMutateWithMin_paramIsSmaller() {
        // Behavior: MIN stores the smaller of the existing value and param (unsigned lexicographic comparison).
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "min-key-1".getBytes();

        zset(channel, cmd, key, longToLE(20));
        zmutate(channel, cmd, key, longToLE(10), ZMutateArgs.Builder.min());

        assertEquals(10, leToLong(zget(channel, cmd, key)));
    }

    @Test
    void shouldMutateWithMin_existingIsSmaller() {
        // Behavior: MIN keeps the existing value when it is already the smaller one.
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "min-key-2".getBytes();

        zset(channel, cmd, key, longToLE(10));
        zmutate(channel, cmd, key, longToLE(20), ZMutateArgs.Builder.min());

        assertEquals(10, leToLong(zget(channel, cmd, key)));
    }

    // --- BYTE_MIN ---

    @Test
    void shouldMutateWithByteMin() {
        // Behavior: BYTE_MIN keeps the lexicographically smaller value.
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "bytemin-key".getBytes();

        byte[] initial = new byte[]{0x10, (byte) 0xFF};
        byte[] param = new byte[]{0x05, 0x01};

        zset(channel, cmd, key, initial);
        zmutate(channel, cmd, key, param, ZMutateArgs.Builder.byteMin());

        assertArrayEquals(param, zget(channel, cmd, key));
    }

    // --- BYTE_MAX ---

    @Test
    void shouldMutateWithByteMax() {
        // Behavior: BYTE_MAX keeps the lexicographically larger value.
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "bytemax-key".getBytes();

        byte[] initial = new byte[]{0x05, 0x01};
        byte[] param = new byte[]{0x10, (byte) 0xFF};

        zset(channel, cmd, key, initial);
        zmutate(channel, cmd, key, param, ZMutateArgs.Builder.byteMax());

        assertArrayEquals(param, zget(channel, cmd, key));
    }

    // --- APPEND_IF_FITS ---

    @Test
    void shouldMutateWithAppendIfFits() {
        // Behavior: APPEND_IF_FITS appends param to the existing value.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset("append-key", "hello").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zmutate("append-key", "-world", ZMutateArgs.Builder.appendIfFits()).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zget("append-key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage msg = (FullBulkStringRedisMessage) response;
            byte[] result = new byte[msg.content().readableBytes()];
            msg.content().readBytes(result);
            assertEquals("hello-world", new String(result));
        }
    }

    // --- SET_VERSIONSTAMPED_VALUE ---

    @Test
    void shouldMutateWithSetVersionstampedValue() {
        // Behavior: SET_VERSIONSTAMPED_VALUE writes a versionstamp into the value at the offset specified by a 4-byte little-endian trailer.
        ZMapCommandBuilder<byte[], byte[]> cmd = new ZMapCommandBuilder<>(ByteArrayCodec.INSTANCE);
        EmbeddedChannel channel = getChannel();
        byte[] key = "vs-value-key".getBytes();

        // param = 10 zero bytes (placeholder for versionstamp) + 4-byte LE offset (0 = start of value)
        byte[] param = new byte[14];
        ByteBuffer.wrap(param, 10, 4).order(ByteOrder.LITTLE_ENDIAN).putInt(0);

        zmutate(channel, cmd, key, param, ZMutateArgs.Builder.setVersionStampedValue());

        byte[] result = zget(channel, cmd, key);
        assertEquals(10, result.length);
    }

    // --- COMPARE_AND_CLEAR ---

    @Test
    void shouldMutateWithCompareAndClear() {
        // Behavior: COMPARE_AND_CLEAR clears the key if its current value equals param.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset("cac-key", "my-value").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zmutate("cac-key", "my-value", ZMutateArgs.Builder.compareAndClear()).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zget("cac-key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, response);
        }
    }

    @Test
    void shouldMutateWithCompareAndClear_mismatch() {
        // Behavior: COMPARE_AND_CLEAR is a no-op when param does not match the stored value.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset("cac-mismatch-key", "abc").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zmutate("cac-mismatch-key", "xyz", ZMutateArgs.Builder.compareAndClear()).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zget("cac-mismatch-key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage msg = (FullBulkStringRedisMessage) response;
            assertNotEquals(FullBulkStringRedisMessage.NULL_INSTANCE, msg);
            byte[] result = new byte[msg.content().readableBytes()];
            msg.content().readBytes(result);
            assertEquals("abc", new String(result));
        }
    }

    // --- Error case ---

    @Test
    void shouldReturnErrorForInvalidMutationType() {
        // Behavior: An invalid mutation type string causes an error response.
        EmbeddedChannel channel = getChannel();

        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.ASCII)
                .addKey("some-key")
                .addValue("some-value")
                .add("INVALID_TYPE");
        Command<String, String, String> rawCmd = new Command<>(CommandType.ZMUTATE, new StatusOutput<>(StringCodec.ASCII), args);

        ByteBuf buf = Unpooled.buffer();
        rawCmd.encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
    }
}
