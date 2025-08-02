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
    public void test_COMMIT() {
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
    public void test_COMMIT_RETURNING_COMMITTED_VERSION() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.commit(CommitArgs.Builder.returning(CommitKeyword.COMMITTED_VERSION)).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$6").
                append("COMMIT").
                append("$9").
                append("RETURNING").
                append("$17").
                append("committed-version");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void test_COMMIT_RETURNING_VERSIONSTAMP() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.commit(CommitArgs.Builder.returning(CommitKeyword.VERSIONSTAMP)).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$6").
                append("COMMIT").
                append("$9").
                append("RETURNING").
                append("$12").
                append("versionstamp");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void test_COMMIT_RETURNING_FUTURES() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.commit(CommitArgs.Builder.returning(CommitKeyword.FUTURES)).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$6").
                append("COMMIT").
                append("$9").
                append("RETURNING").
                append("$7").
                append("futures");
        assertEquals(expectedCommand.toString(), new String(command));
    }

    @Test
    public void test_COMMIT_RETURNING_ALL() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.commit(CommitArgs.Builder.returning(CommitKeyword.FUTURES, CommitKeyword.VERSIONSTAMP, CommitKeyword.COMMITTED_VERSION)).encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*5").
                append("$6").
                append("COMMIT").
                append("$9").
                append("RETURNING").
                append("$7").
                append("futures").
                append("$12").
                append("versionstamp").
                append("$17").
                append("committed-version");
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
        cmd.namespaceCreate("my-namespace").encode(buf);

        byte[] command = new byte[buf.readableBytes()];
        buf.readBytes(command);
        RESPCommandBuilder expectedCommand = new RESPCommandBuilder().
                append("*3").
                append("$9").
                append("NAMESPACE").
                append("$6").
                append("CREATE").
                append("$12").
                append("my-namespace");
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
                append("$12").
                append("SNAPSHOTREAD").
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
                append("$12").
                append("SNAPSHOTREAD").
                append("$3").
                append("OFF");
        assertEquals(expectedCommand.toString(), new String(command));
    }
}