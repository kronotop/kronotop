/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.kronotop.volume.Session;
import com.kronotop.volume.replication.BaseNetworkedVolumeTest;
import com.kronotop.volume.segment.SegmentAnalysis;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SegmentRangeHandlerTest extends BaseNetworkedVolumeTest {

    @Test
    public void test_SEGMENTRANGE() throws IOException {
        ByteBuffer[] entries = {
                ByteBuffer.wrap(new byte[]{1, 2, 3}),
                ByteBuffer.wrap(new byte[]{4, 5, 6}),
        };
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        String segmentName = segmentAnalysis.getFirst().name();

        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        SegmentRange[] ranges = {
                new SegmentRange(0, 3),
                new SegmentRange(3, 3),
        };
        cmd.segmentrange(volumeConfig.name(), segmentName, ranges).encode(buf);
        kronotopInstance.getChannel().writeInbound(buf);

        Object response = kronotopInstance.getChannel().readOutbound();
        ArrayRedisMessage message = (ArrayRedisMessage) response;
        for (int i = 0; i < ranges.length; i++) {
            FullBulkStringRedisMessage redisMessage = (FullBulkStringRedisMessage) message.children().get(i);
            assertArrayEquals(entries[i].array(), redisMessage.content().array());
        }
    }

    @Test
    public void test_SEGMENTRANGE_VolumeNotOpenException() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentrange("foobar", "barfoo", new SegmentRange(0, 3)).encode(buf);
        kronotopInstance.getChannel().writeInbound(buf);

        Object response = kronotopInstance.getChannel().readOutbound();
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Volume: 'foobar' is not open", message.content());
    }

    @Test
    public void test_SEGMENTRANGE_SegmentNotFoundException() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentrange(volumeConfig.name(), "0000000000000000001", new SegmentRange(0, 3)).encode(buf);
        kronotopInstance.getChannel().writeInbound(buf);

        Object response = kronotopInstance.getChannel().readOutbound();
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Segment: '0000000000000000001' could not be found", message.content());
    }
}