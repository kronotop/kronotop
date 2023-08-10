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

package com.kronotop.protocol;

import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ZMapTest {
    @Test
    public void testZSET() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.zput("foobar.barfoo", "key", "value").encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*4").
                append("$4").
                append("ZPUT").
                append("$13").
                append("foobar.barfoo").
                append("$3").
                append("key").
                append("$5").
                append("value");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testZGET() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.zget("foobar.barfoo", "key").encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$4").
                append("ZGET").
                append("$13").
                append("foobar.barfoo").
                append("$3").
                append("key");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testZDEL() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.zdel("foobar.barfoo", "key").encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$4").
                append("ZDEL").
                append("$13").
                append("foobar.barfoo").
                append("$3").
                append("key");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testZDELPREFIX() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.zdelprefix("prefix".getBytes()).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*2").
                append("$10").
                append("ZDELPREFIX").
                append("$6").
                append("prefix");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testZDELRANGE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        ZDelRangeArgs zDelRangeArgs = ZDelRangeArgs.Builder.begin("begin".getBytes()).end("end".getBytes());
        cmd.zdelrange("foobar.barfoo", zDelRangeArgs).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*4").
                append("$9").
                append("ZDELRANGE").
                append("$13").
                append("foobar.barfoo").
                append("$5").
                append("begin").
                append("$3").
                append("end");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testZGETRANGE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        ZGetRangeArgs zGetRangeArgs = ZGetRangeArgs.Builder.begin("begin".getBytes()).end("end".getBytes());
        cmd.zgetrange("foobar.barfoo", zGetRangeArgs).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*4").
                append("$9").
                append("ZGETRANGE").
                append("$13").
                append("foobar.barfoo").
                append("$5").
                append("begin").
                append("$3").
                append("end");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testZGETRANGE_Arguments() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        ZGetRangeArgs zGetRangeArgs = ZGetRangeArgs.Builder.
                begin("begin".getBytes()).
                end("end".getBytes()).
                limit(3).
                reverse().
                beginKeySelector("FIRST_GREATER_THAN")
                .endKeySelector("LAST_LESS_OR_EQUAL");
        cmd.zgetrange("foobar.barfoo", zGetRangeArgs).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*11").
                append("$9").
                append("ZGETRANGE").
                append("$13").
                append("foobar.barfoo").
                append("$5").
                append("begin").
                append("$3").
                append("end").
                append("$5").
                append("LIMIT").
                append("$1").
                append("3").
                append("$7").
                append("REVERSE").
                append("$18").
                append("BEGIN_KEY_SELECTOR").
                append("$18").
                append("FIRST_GREATER_THAN").
                append("$16").
                append("END_KEY_SELECTOR").
                append("$18").
                append("LAST_LESS_OR_EQUAL");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testZGETKEY() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        ZGetKeyArgs zGetKeyArgs = ZGetKeyArgs.Builder.key("key".getBytes()).keySelector("last_less_than");
        cmd.zgetkey("foobar.barfoo", zGetKeyArgs).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*5").
                append("$7").
                append("ZGETKEY").
                append("$13").
                append("foobar.barfoo").
                append("$3").
                append("key").
                append("$12").
                append("KEY_SELECTOR").
                append("$14").
                append("last_less_than");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testZMUTATE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.add();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$3").
                    append("ADD");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.bitAnd();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$7").
                    append("BIT_AND");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.bitOr();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$6").
                    append("BIT_OR");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.bitXor();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$7").
                    append("BIT_XOR");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.appendIfFits();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$14").
                    append("APPEND_IF_FITS");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.max();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$3").
                    append("MAX");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.min();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$3").
                    append("MIN");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.setVersionStampedKey();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$22").
                    append("SET_VERSIONSTAMPED_KEY");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.setVersionStampedValue();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$24").
                    append("SET_VERSIONSTAMPED_VALUE");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.byteMin();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$8").
                    append("BYTE_MIN");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.byteMax();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$8").
                    append("BYTE_MAX");
            assertEquals(expectedCommand.toString(), new String(command));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            ZMutateArgs zMutateArgs = ZMutateArgs.Builder.compareAndClear();
            cmd.zmutate("foobar.barfoo", "key", "param", zMutateArgs).encode(buf);

            byte[] command = new byte[buf.readableBytes()];
            buf.readBytes(command);
            RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                    append("*5").
                    append("$7").
                    append("ZMUTATE").
                    append("$13").
                    append("foobar.barfoo").
                    append("$3").
                    append("key").
                    append("$5").
                    append("param").
                    append("$17").
                    append("COMPARE_AND_CLEAR");
            assertEquals(expectedCommand.toString(), new String(command));
        }
    }

    @Test
    public void testZGETRANGESIZE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        ZGetRangeSizeArgs zGetRangeSizeArgs = ZGetRangeSizeArgs.Builder.begin("begin".getBytes()).end("end".getBytes());
        cmd.zgetrangesize("foobar.barfoo", zGetRangeSizeArgs).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*4").
                append("$13").
                append("ZGETRANGESIZE").
                append("$13").
                append("foobar.barfoo").
                append("$5").
                append("begin").
                append("$3").
                append("end");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testGETAPPROXIMATESIZE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.getapproximatesize().encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*1").
                append("$18").
                append("GETAPPROXIMATESIZE");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testGETREADVERSION() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.getreadversion().encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*1").
                append("$14").
                append("GETREADVERSION");
        assertEquals(expectedCommand.toString(), new String(command));
    }
}
