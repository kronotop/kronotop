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

import com.kronotop.metrics.NetworkMetrics;
import com.kronotop.metrics.RuntimeMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class NettyTrafficCounterTest {
    private static final byte[] PING = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PONG = "+PONG\r\n".getBytes(StandardCharsets.US_ASCII);

    private NetworkMetrics metrics;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        RuntimeMetrics runtimeMetrics = new RuntimeMetrics();
        metrics = runtimeMetrics.getNetworkMetrics();
        channel = new EmbeddedChannel(new NettyTrafficCounter(ServerKind.EXTERNAL, runtimeMetrics));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldCountInboundBytes() {
        // Behavior: every inbound ByteBuf adds its readable bytes to read_bytes of the server kind
        // and passes through the pipeline unchanged
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(PING)));

        assertEquals(PING.length, metrics.getReadBytes(ServerKind.EXTERNAL));
        assertEquals(0, metrics.getWrittenBytes(ServerKind.EXTERNAL));
        ByteBuf passed = channel.readInbound();
        assertEquals(PING.length, passed.readableBytes());
        passed.release();
    }

    @Test
    void shouldCountOutboundBytes() {
        // Behavior: every outbound ByteBuf adds its readable bytes to written_bytes of the server kind
        // and passes through the pipeline unchanged
        assertTrue(channel.writeOutbound(Unpooled.wrappedBuffer(PONG)));

        assertEquals(PONG.length, metrics.getWrittenBytes(ServerKind.EXTERNAL));
        assertEquals(0, metrics.getReadBytes(ServerKind.EXTERNAL));
        ByteBuf passed = channel.readOutbound();
        assertEquals(PONG.length, passed.readableBytes());
        passed.release();
    }

    @Test
    void shouldAccumulateAcrossWrites() {
        // Behavior: the counters keep growing over the life of the channel
        channel.writeInbound(Unpooled.wrappedBuffer(PING));
        channel.writeInbound(Unpooled.wrappedBuffer(PING));
        channel.writeOutbound(Unpooled.wrappedBuffer(PONG));
        channel.writeOutbound(Unpooled.wrappedBuffer(PONG));

        assertEquals(2L * PING.length, metrics.getReadBytes(ServerKind.EXTERNAL));
        assertEquals(2L * PONG.length, metrics.getWrittenBytes(ServerKind.EXTERNAL));
    }

    @Test
    void shouldIgnoreNonByteBufMessages() {
        // Behavior: a message that is not a ByteBuf is passed on without touching the counters
        assertTrue(channel.writeInbound("inbound"));
        assertTrue(channel.writeOutbound("outbound"));

        assertEquals(0, metrics.getReadBytes(ServerKind.EXTERNAL));
        assertEquals(0, metrics.getWrittenBytes(ServerKind.EXTERNAL));
        assertEquals("inbound", channel.readInbound());
        assertEquals("outbound", channel.readOutbound());
    }

    @Test
    void shouldNotTouchOtherServerKind() {
        // Behavior: a counter bound to EXTERNAL never changes the INTERNAL counters
        channel.writeInbound(Unpooled.wrappedBuffer(PING));
        channel.writeOutbound(Unpooled.wrappedBuffer(PONG));

        assertEquals(0, metrics.getReadBytes(ServerKind.INTERNAL));
        assertEquals(0, metrics.getWrittenBytes(ServerKind.INTERNAL));
    }
}
