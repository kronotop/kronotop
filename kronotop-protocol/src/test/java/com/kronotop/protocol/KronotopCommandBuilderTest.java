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

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KronotopCommandBuilderTest {
    @Test
    public void testAuth() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.auth("devuser", "devpass").encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$4").
                append("AUTH").
                append("$7").
                append("devuser").
                append("$7").
                append("devpass");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testBegin() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.begin().encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*1").
                append("$5").
                append("BEGIN");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testRollback() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.rollback().encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*1").
                append("$8").
                append("ROLLBACK");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testCommit() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.commit().encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*1").
                append("$6").
                append("COMMIT");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testCommitAndGetCommittedVersion() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.commitAndGetCommittedVersion().encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*2").
                append("$6").
                append("COMMIT").
                append("$17").
                append("committed-version");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testCommitAndGetVersionstamp() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.commitAndGetVersionstamp().encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*2").
                append("$6").
                append("COMMIT").
                append("$12").
                append("versionstamp");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testNamespaceList() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceList("my-namespace").encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$9").
                append("NAMESPACE").
                append("$4").
                append("LIST").
                append("$12").
                append("my-namespace");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testNamespaceCreate() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        NamespaceArgs args = NamespaceArgs.Builder.layer("my-layer".getBytes()).prefix("my-prefix".getBytes());
        cmd.namespaceCreate("my-namespace", args).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*7").
                append("$9").
                append("NAMESPACE").
                append("$6").
                append("CREATE").
                append("$12").
                append("my-namespace").
                append("$5").
                append("LAYER").
                append("$8").
                append("my-layer").
                append("$6").
                append("PREFIX").
                append("$9").
                append("my-prefix");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testNamespaceExists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceExists("my-namespace").encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$9").
                append("NAMESPACE").
                append("$6").
                append("EXISTS").
                append("$12").
                append("my-namespace");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testNamespaceMove() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceMove("old", "new").encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*4").
                append("$9").
                append("NAMESPACE").
                append("$4").
                append("MOVE").
                append("$3").
                append("old").
                append("$3").
                append("new");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testNamespaceRemove() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceRemove("my-namespace").encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$9").
                append("NAMESPACE").
                append("$6").
                append("REMOVE").
                append("$12").
                append("my-namespace");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testSnapshotRead_ON() {
        KronotopCommandBuilder<byte[], byte[]> cmd = new KronotopCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*2").
                append("$13").
                append("SNAPSHOT_READ").
                append("$2").
                append("ON");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void testSnapshotRead_OFF() {
        KronotopCommandBuilder<byte[], byte[]> cmd = new KronotopCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.snapshotRead(SnapshotReadArgs.Builder.off()).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*2").
                append("$13").
                append("SNAPSHOT_READ").
                append("$3").
                append("OFF");
        assertEquals(expectedCommand.toString(), new String(command));
    }
}