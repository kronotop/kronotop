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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.client.protocol.InternalCommandBuilder;
import com.kronotop.cluster.client.protocol.PackedEntry;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.*;
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

class SegmentInsertHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @Test
    void shouldInsertEntriesIntoSegment() throws IOException {
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6};
        ByteBuffer[] entries = {
                ByteBuffer.wrap(first),
                ByteBuffer.wrap(second),
        };
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        KronotopTestInstance secondInstance = addNewInstance();
        // SEGMEN.TINSERT requires an already opened volume to run.
        VolumeConfigGenerator generator = new VolumeConfigGenerator(secondInstance.getContext(), ShardKind.REDIS, 1);
        VolumeService volumeService = secondInstance.getContext().getService(VolumeService.NAME);
        Volume secondVolume = volumeService.newVolume(generator.volumeConfig());


        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        long segmentId = segmentAnalysis.getFirst().segmentId();

        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        PackedEntry[] packedEntries = {
                new PackedEntry(0, first),
                new PackedEntry(3, second),
        };

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentinsert(volumeConfig.name(), segmentId, packedEntries).encode(buf);
        Object response = runCommand(secondInstance.getChannel(), buf);

        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage message = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, message.content());

        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            Versionstamp[] versionstamps = result.getVersionstampedKeys();

            ByteBuffer firstBuffer = secondVolume.get(session, versionstamps[0]);
            assertArrayEquals(first, firstBuffer.array());

            ByteBuffer secondBuffer = secondVolume.get(session, versionstamps[1]);
            assertArrayEquals(second, secondBuffer.array());
        }
    }

    @Test
    void shouldThrowVolumeNotOpenExceptionWhenVolumeNotExists() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentinsert("foobar", 1, new PackedEntry(0, new byte[]{0, 1, 2})).encode(buf);
        Object response = runCommand(kronotopInstance.getChannel(), buf);

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Volume: 'foobar' is not open", message.content());
    }

    @Test
    void shouldThrowSegmentNotFoundExceptionWhenSegmentNotExists() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentinsert(volumeConfig.name(), 1, new PackedEntry(0, new byte[]{0, 1, 2})).encode(buf);
        Object response = runCommand(kronotopInstance.getChannel(), buf);

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Segment with id: '1' could not be found", message.content());
    }
}