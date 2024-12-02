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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.client.protocol.InternalCommandBuilder;
import com.kronotop.cluster.client.protocol.PackedEntry;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.*;
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

class SegmentInsertHandlerTest extends BaseNetworkedVolumeTest {

    @Test
    public void test_SEGMENTINSERT() throws IOException {
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6};
        ByteBuffer[] entries = {
                ByteBuffer.wrap(first),
                ByteBuffer.wrap(second),
        };
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        KronotopTestInstance secondInstance = addNewInstance();
        // SEGMENTINSERT requires an already opened volume to run.
        VolumeConfigGenerator generator = new VolumeConfigGenerator(secondInstance.getContext(), ShardKind.REDIS, 1);
        VolumeService volumeService = secondInstance.getContext().getService(VolumeService.NAME);
        Volume secondVolume = volumeService.newVolume(generator.volumeConfig());


        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        String segmentName = segmentAnalysis.getFirst().name();

        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        PackedEntry[] packedEntries = {
                new PackedEntry(0, first),
                new PackedEntry(3, second),
        };
        ByteBuf buf = Unpooled.buffer();
        cmd.segmentinsert(volumeConfig.name(), segmentName, packedEntries).encode(buf);
        secondInstance.getChannel().writeInbound(buf);

        Object response = secondInstance.getChannel().readOutbound();
        SimpleStringRedisMessage message = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, message.content());

        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, prefix);
            Versionstamp[] versionstamps = result.getVersionstampedKeys();

            ByteBuffer firstBuffer = secondVolume.get(session, versionstamps[0]);
            assertArrayEquals(first, firstBuffer.array());

            ByteBuffer secondBuffer = secondVolume.get(session, versionstamps[1]);
            assertArrayEquals(second, secondBuffer.array());
        }
    }

    @Test
    public void test_SEGMENTINSERT_VolumeNotOpenException() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentinsert("foobar", "barfoo", new PackedEntry(0, new byte[]{0, 1, 2})).encode(buf);
        kronotopInstance.getChannel().writeInbound(buf);

        Object response = kronotopInstance.getChannel().readOutbound();
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Volume: 'foobar' is not open", message.content());
    }

    @Test
    public void test_SEGMENTINSERT_SegmentNotFoundException() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentinsert(volumeConfig.name(), "0000000000000000001", new PackedEntry(0, new byte[]{0, 1, 2})).encode(buf);
        kronotopInstance.getChannel().writeInbound(buf);

        Object response = kronotopInstance.getChannel().readOutbound();
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Segment: '0000000000000000001' could not be found", message.content());
    }
}