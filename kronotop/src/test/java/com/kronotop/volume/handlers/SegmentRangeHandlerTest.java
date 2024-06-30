package com.kronotop.volume.handlers;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.protocol.InternalCommandBuilder;
import com.kronotop.cluster.protocol.SegmentRange;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.volume.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SegmentRangeHandlerTest extends BaseVolumeTest {
    String volumeName = "test_volume";
    Database database;
    VolumeService service;
    DirectorySubspace directorySubspace;
    Volume volume;
    VolumeConfig volumeConfig;

    @BeforeEach
    public void setupVolumeTestEnvironment() throws IOException {
        database = kronotopInstance.getContext().getFoundationDB();
        service = kronotopInstance.getContext().getService(VolumeService.NAME);
        directorySubspace = getDirectorySubspace();
        volumeConfig = new VolumeConfig(directorySubspace, volumeName);
        volume = service.newVolume(volumeConfig);
    }

    @Test
    public void test_SEGMENTRANGE() throws IOException {
        ByteBuffer[] entries = {
                ByteBuffer.wrap(new byte[]{1, 2, 3}),
                ByteBuffer.wrap(new byte[]{4, 5, 6}),
        };
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            volume.append(session, entries);
            tr.commit().join();
        }

        String segmentName = volume.getStats().getSegments().keySet().iterator().next();

        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        SegmentRange[] ranges = {
                new SegmentRange(0, 3),
                new SegmentRange(3, 3),
        };
        cmd.segmentrange(volumeName, segmentName, ranges).encode(buf);
        channel.writeInbound(buf);

        Object response = channel.readOutbound();
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
        channel.writeInbound(buf);

        Object response = channel.readOutbound();
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Volume: 'foobar' is not open", message.content());
    }

    @Test
    public void test_SEGMENTRANGE_SegmentNotFoundException() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.segmentrange(volumeName, "barfoo", new SegmentRange(0, 3)).encode(buf);
        channel.writeInbound(buf);

        Object response = channel.readOutbound();
        ErrorRedisMessage message = (ErrorRedisMessage) response;
        assertEquals("ERR Segment: 'barfoo' could not be found", message.content());
    }
}