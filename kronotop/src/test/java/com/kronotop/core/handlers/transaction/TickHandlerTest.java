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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.TickArgs;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class TickHandlerTest extends BaseHandlerTest {

    private long runTick(EmbeddedChannel channel, TickArgs args) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.tick(args).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(IntegerRedisMessage.class, response);
        IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
        return actualMessage.value();
    }

    // The committed version advances only when a commit arrives; on an idle cluster
    // consecutive reads return the same version. Commit a dummy write to advance it.
    private void advanceCommittedVersion() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("tick-handler-test");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.set(subspace.pack("tick"), new byte[]{0x01});
            tr.commit().join();
        }
    }

    @Test
    void shouldReturnFreshReadVersion() {
        // Behavior: TICK FRESH opens a new transaction and returns a positive read version
        EmbeddedChannel channel = getChannel();
        long readVersion = runTick(channel, TickArgs.Builder.fresh());
        assertTrue(readVersion > 0);
    }

    @Test
    void shouldReturnCachedReadVersionWithinTtl() {
        // Behavior: the first TICK CACHED populates the cache with a positive read version,
        // a second TICK CACHED issued within one second returns the cached value even if the
        // committed version has advanced in the meantime
        EmbeddedChannel channel = getChannel();
        long first = runTick(channel, TickArgs.Builder.cached());
        assertTrue(first > 0);

        advanceCommittedVersion();

        long second = runTick(channel, TickArgs.Builder.cached());
        assertEquals(first, second);
    }

    @Test
    void shouldRefreshCachedReadVersionAfterTtl() throws InterruptedException {
        // Behavior: TICK CACHED refreshes the cache after the one second TTL expires; a commit
        // made in the meantime advances the committed version, so the refreshed value is
        // strictly greater than the first one
        EmbeddedChannel channel = getChannel();
        long first = runTick(channel, TickArgs.Builder.cached());

        Thread.sleep(1100);
        advanceCommittedVersion();

        long second = runTick(channel, TickArgs.Builder.cached());
        assertTrue(second > first);
    }

    @Test
    void shouldAcceptLowercaseMode() {
        // Behavior: TICK accepts case-insensitive mode arguments, lowercase "cached" works
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$4\r\nTICK\r\n$6\r\ncached\r\n".getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(channel, buf);
        assertInstanceOf(IntegerRedisMessage.class, response);
        IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
        assertTrue(actualMessage.value() > 0);
    }

    @Test
    void shouldRejectInvalidMode() {
        // Behavior: TICK with an unknown mode returns an error
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$4\r\nTICK\r\n$3\r\nFOO\r\n".getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertTrue(actualMessage.content().contains("illegal argument for TICK: 'FOO'"));
    }

    @Test
    void shouldRejectMissingParameter() {
        // Behavior: TICK without a mode parameter returns a wrong number of arguments error
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*1\r\n$4\r\nTICK\r\n".getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertTrue(actualMessage.content().contains("wrong number of arguments"));
    }
}
