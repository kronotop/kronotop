/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.client.protocol.InternalCommandBuilder;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeSession;
import com.kronotop.volume.segment.SegmentAnalysis;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SegmentTailPointerHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @Test
    void shouldReturnZeroPositionAndNegativeSequenceNumber_whenSegmentIsEmpty() throws IOException {
        // Write data to create a segment, then query with that segment
        ByteBuffer[] entries = {ByteBuffer.wrap(new byte[]{1, 2, 3})};
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        long segmentId = segmentAnalysis.getFirst().segmentId();

        // Query a different segment ID that doesn't exist (empty segment)
        long emptySegmentId = segmentId + 100;

        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.segmentTailPointer(volumeConfig.name(), emptySegmentId).encode(buf);

        Object response = runCommand(kronotopInstance.getChannel(), buf);
        assertInstanceOf(ArrayRedisMessage.class, response);

        ArrayRedisMessage message = (ArrayRedisMessage) response;
        assertEquals(2, message.children().size());

        IntegerRedisMessage nextPosition = (IntegerRedisMessage) message.children().get(0);
        IntegerRedisMessage sequenceNumber = (IntegerRedisMessage) message.children().get(1);

        assertEquals(0, nextPosition.value());
        assertEquals(-1, sequenceNumber.value());
    }

    @Test
    void shouldReturnPositionAndSequenceNumber_whenSegmentHasData() throws IOException {
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6};
        ByteBuffer[] entries = {
                ByteBuffer.wrap(first),
                ByteBuffer.wrap(second),
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
        cmd.segmentTailPointer(volumeConfig.name(), segmentId).encode(buf);

        Object response = runCommand(kronotopInstance.getChannel(), buf);
        assertInstanceOf(ArrayRedisMessage.class, response);

        ArrayRedisMessage message = (ArrayRedisMessage) response;
        assertEquals(2, message.children().size());

        IntegerRedisMessage nextPosition = (IntegerRedisMessage) message.children().get(0);
        IntegerRedisMessage sequenceNumber = (IntegerRedisMessage) message.children().get(1);

        long expectedNextPosition = first.length + second.length;
        assertEquals(expectedNextPosition, nextPosition.value());
        assertTrue(sequenceNumber.value() > 0);
    }

    @Test
    void shouldReturnZeroPositionAndNegativeSequenceNumber_whenQueryingSegmentZeroOnEmptyVolume() {
        // Query segment ID 0 on an empty volume (no data written yet)
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.segmentTailPointer(volumeConfig.name(), 0L).encode(buf);

        Object response = runCommand(kronotopInstance.getChannel(), buf);
        assertInstanceOf(ArrayRedisMessage.class, response);

        ArrayRedisMessage message = (ArrayRedisMessage) response;
        assertEquals(2, message.children().size());

        IntegerRedisMessage nextPosition = (IntegerRedisMessage) message.children().get(0);
        IntegerRedisMessage sequenceNumber = (IntegerRedisMessage) message.children().get(1);

        assertEquals(0, nextPosition.value());
        assertEquals(-1, sequenceNumber.value());
    }
}