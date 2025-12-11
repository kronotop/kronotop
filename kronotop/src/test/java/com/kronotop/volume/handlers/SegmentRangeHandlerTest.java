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

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.client.protocol.InternalCommandBuilder;
import com.kronotop.cluster.client.protocol.SegmentRange;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.volume.VolumeSession;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.segment.SegmentAnalysis;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SegmentRangeHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @Test
    void shouldRetrieveMultipleSegmentRanges() throws IOException {
        ByteBuffer[] entries = {
                ByteBuffer.wrap(new byte[]{1, 2, 3}),
                ByteBuffer.wrap(new byte[]{4, 5, 6}),
        };
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        long segmentId = segmentAnalysis.getFirst().segmentId();

        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        List<SegmentRange> ranges = List.of(
                new SegmentRange(0, 3),
                new SegmentRange(3, 3)
        );
        cmd.segmentrange(volumeConfig.name(), segmentId, ranges).encode(buf);

        Object response = runCommand(kronotopInstance.getChannel(), buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage message = (ArrayRedisMessage) response;
        for (int i = 0; i < ranges.size(); i++) {
            FullBulkStringRedisMessage redisMessage = (FullBulkStringRedisMessage) message.children().get(i);
            assertArrayEquals(entries[i].array(), redisMessage.content().array());
        }
    }

    @Test
    void shouldThrowVolumeNotOpenExceptionWhenVolumeNotExists() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentrange("foobar", 1, List.of(new SegmentRange(0, 3))).encode(buf);

        Object response = runCommand(kronotopInstance.getChannel(), buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Volume: 'foobar' is not open", message.content());
    }

    @Test
    void shouldThrowSegmentNotFoundExceptionWhenSegmentNotExists() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentrange(volumeConfig.name(), 1, List.of(new SegmentRange(0, 3))).encode(buf);

        Object response = runCommand(kronotopInstance.getChannel(), buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Segment with id: '1' could not be found", message.content());
    }

    @Test
    void shouldThrowEntryOutOfBoundExceptionWhenRangeExceedsSegmentSize() throws IOException {
        ByteBuffer[] entries = {
                ByteBuffer.wrap(new byte[]{1, 2, 3}),
        };
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        long segmentId = segmentAnalysis.getFirst().segmentId();
        long segmentSize = segmentAnalysis.getFirst().size();

        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        List<SegmentRange> ranges = List.of(new SegmentRange(segmentSize - 10, 20));
        cmd.segmentrange(volumeConfig.name(), segmentId, ranges).encode(buf);

        Object response = runCommand(kronotopInstance.getChannel(), buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("OUTOFBOUND position: " + (segmentSize - 10) + ", length: 20 but size: " + segmentSize, message.content());
    }
}