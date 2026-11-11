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

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.commands.VolumeAdminCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.*;
import com.kronotop.volume.replication.ReplicationService;
import com.kronotop.volume.segment.SegmentUtil;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VolumeAdminHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    private final String volumeName = VolumeNames.format(SHARD_ID);

    @BeforeEach
    void useRESP3() {
        // These tests assert RESP3 types, so the session must speak RESP3.
        switchProtocol(channel, RESPVersion.RESP3);
    }

    private void injectTestData() throws IOException {
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }
    }

    @Test
    void shouldListVolumes() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.list().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        // the name and number of the volumes is an implementation detail of different components
        assertFalse(actualMessage.children().isEmpty());
    }

    @Test
    void shouldDescribeVolume() throws IOException {
        injectTestData();

        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.describe("shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        actualMessage.children().forEach((k, v) -> {
            String key = ((FullBulkStringRedisMessage) k).content().toString(StandardCharsets.UTF_8);
            switch (key) {
                case "name":
                    assertEquals("shard-1", ((FullBulkStringRedisMessage) v).content().toString(StandardCharsets.UTF_8));
                    break;
                case "status":
                    assertEquals(VolumeStatus.READWRITE.name(), ((FullBulkStringRedisMessage) v).content().toString(StandardCharsets.UTF_8));
                    break;
                case "data_dir":
                    FullBulkStringRedisMessage dataDir = (FullBulkStringRedisMessage) v;
                    assertFalse(dataDir.content().toString(StandardCharsets.UTF_8).isEmpty());
                    break;
                case "segment_size":
                    assert v instanceof IntegerRedisMessage;
                    IntegerRedisMessage segmentSize = (IntegerRedisMessage) v;
                    assertTrue(segmentSize.value() > 0);
                    break;
                case "segments":
                    MapRedisMessage segments = (MapRedisMessage) v;
                    assertFalse(segments.children().isEmpty());
                    segments.children().forEach((kk, vv) -> {
                        assertInstanceOf(IntegerRedisMessage.class, kk);

                        MapRedisMessage sub = (MapRedisMessage) vv;
                        sub.children().forEach((k2, v2) -> {
                            String segmentKey = ((FullBulkStringRedisMessage) k2).content().toString(StandardCharsets.UTF_8);
                            switch (segmentKey) {
                                case "size", "free_bytes", "used_bytes", "cardinality":
                                    IntegerRedisMessage size = (IntegerRedisMessage) v2;
                                    assertTrue(size.value() > 0);
                                    break;
                                case "garbage_percentage":
                                    // TODO: Waits for KR-9
                                    break;
                            }
                        });
                    });
            }
        });
    }

    @Test
    void shouldReturnErrorWhenDescribeVolumeWithInvalidNumberOfArguments() {
        // Send command without volume name argument
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$12\r\nVOLUME.ADMIN\r\n$8\r\nDESCRIBE\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Wrong number of arguments for 'VOLUME.ADMIN DESCRIBE' command"));
    }

    @Test
    void shouldReturnErrorWhenDescribeVolumeNotFound() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.describe("non-existent-volume").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Volume: 'non-existent-volume' is not open"));
    }

    @Test
    void shouldListSegmentsEmptyVolume() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.listSegments("shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        // Creates a new default one
        assertEquals(1, actualMessage.children().size());
    }

    @Test
    void shouldListSegments() throws IOException {
        injectTestData();
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.listSegments("shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());
        for (RedisMessage child : actualMessage.children()) {
            IntegerRedisMessage message = (IntegerRedisMessage) child;
            assertEquals(0L, message.value());
        }
    }

    @Test
    void shouldSetVolumeStatus() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setStatus("shard-1", "READONLY").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.describe("shard-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            actualMessage.children().forEach((k, v) -> {
                String key = ((FullBulkStringRedisMessage) k).content().toString(StandardCharsets.UTF_8);
                if (key.equals("status")) {
                    assertEquals(VolumeStatus.READONLY.name(), ((FullBulkStringRedisMessage) v).content().toString(StandardCharsets.UTF_8));
                }
            });
        }
    }

    @Test
    void shouldReturnErrorWhenSetStatusWithInvalidNumberOfArguments() {
        // Send command without status argument
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$12\r\nVOLUME.ADMIN\r\n$10\r\nSET-STATUS\r\n$7\r\nshard-1\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Wrong number of arguments for 'VOLUME.ADMIN SET-STATUS' command"));
    }

    @Test
    void shouldReturnErrorWhenSetStatusVolumeNotFound() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setStatus("non-existent-volume", "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Volume: 'non-existent-volume' is not open"));
    }

    @Test
    void shouldReturnErrorWhenSetStatusWithInvalidStatus() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setStatus("shard-1", "INVALID").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Unknown volume status: 'INVALID'", errorMessage.content());
    }

    @Test
    void shouldCleanupOrphanFiles() throws IOException {
        ByteBuffer[] entries = getEntries(3, 10);

        VolumeService service = context.getService(VolumeService.NAME);
        Volume shard = service.findVolume("shard-1");
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            shard.append(session, entries);
            tr.commit().join();
        }
        File orphanFile = new File(String.valueOf(Paths.get(shard.getConfig().dataDir(), SegmentUtil.DIRECTORY, "orphan-file")));
        assertTrue(orphanFile.createNewFile());

        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.cleanupOrphanFiles("shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        RedisMessage message = actualMessage.children().getFirst();
        FullBulkStringRedisMessage path = (FullBulkStringRedisMessage) message;
        assertEquals(orphanFile.getAbsolutePath(), path.content().toString(StandardCharsets.UTF_8));
        // Deleted
        assertFalse(orphanFile.exists());
    }

    private void setStandbyMember(KronotopTestInstance standby) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", SHARD_ID, standby.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        RoutingService routing = standby.getContext().getService(RoutingService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Route route = routing.findRoute(SHARD_ID);
            if (route == null) {
                return false;
            }
            return route.standbys().contains(standby.getMember());
        });
    }

    private void stopReplication(KronotopTestInstance standby) {
        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        replicationService.stopReplication(SHARD_ID, true);
    }

    @Test
    void shouldStartReplicationSuccessfully() {
        KronotopTestInstance standby = addNewInstance();
        setStandbyMember(standby);

        // Stop the auto-started replication first
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
            List<Integer> shardIds = replicationService.listActiveReplications();
            return shardIds.contains(SHARD_ID);
        });
        stopReplication(standby);

        // Verify replication is stopped
        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        assertTrue(replicationService.listActiveReplications().isEmpty());

        // Start replication via command
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.startReplication(volumeName).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify replication has started
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            List<Integer> shardIds = replicationService.listActiveReplications();
            return shardIds.contains(SHARD_ID);
        });
    }

    @Test
    void shouldFailStartReplicationWhenNoRouteFound() {
        KronotopTestInstance standby = addNewInstance();

        // Try to start replication for a shard that has no route (standby not assigned)
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.startReplication(volumeName).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for 'shard-1'", actualMessage.content());
    }

    @Test
    void shouldFailStartReplicationWhenNodeIsNotStandby() {
        // Try to start replication on primary node (not a standby)
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.startReplication(volumeName).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for 'shard-1'", actualMessage.content());
    }

    @Test
    void shouldFailStartReplicationWhenShardIdIsNotAnInteger() {
        // Behavior: VOLUME.ADMIN REPLICATION START with the volume name "shard-abc" replies with the error
        // "ERR Invalid shard id: 'abc'", because the part after "shard-" is not an integer.
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.startReplication("shard-abc").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid shard id: 'abc'", actualMessage.content());
    }

    @Test
    void shouldFailStartReplicationWithInvalidNumberOfArguments() {
        // Missing volume-name argument
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$12\r\nVOLUME.ADMIN\r\n$11\r\nREPLICATION\r\n$5\r\nSTART\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'VOLUME.ADMIN REPLICATION' command", actualMessage.content());
    }

    @Test
    void shouldStopReplicationSuccessfully() {
        KronotopTestInstance standby = addNewInstance();
        setStandbyMember(standby);

        // Wait for replication to start
        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            List<Integer> shardIds = replicationService.listActiveReplications();
            return shardIds.contains(SHARD_ID);
        });

        // Stop replication via command
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.stopReplication(volumeName).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify replication has stopped
        assertTrue(replicationService.listActiveReplications().isEmpty());
    }

    @Test
    void shouldFailStopReplicationWhenNoRouteFound() {
        KronotopTestInstance standby = addNewInstance();

        // Try to stop replication for a shard that has no route (standby not assigned)
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.stopReplication(volumeName).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for 'shard-1'", actualMessage.content());
    }

    @Test
    void shouldFailStopReplicationWhenNodeIsNotStandby() {
        // Try to stop replication on primary node (not a standby)
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.stopReplication(volumeName).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for 'shard-1'", actualMessage.content());
    }

    @Test
    void shouldFailStopReplicationWithInvalidNumberOfArguments() {
        // Missing volume-name argument
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$12\r\nVOLUME.ADMIN\r\n$11\r\nREPLICATION\r\n$4\r\nSTOP\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'VOLUME.ADMIN REPLICATION' command", actualMessage.content());
    }

    @Test
    void shouldPruneChangelogSuccessfully() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("shard-1", 24).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogRetentionPeriodIsZero() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("shard-1", 0).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Retention period must be greater than zero", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogRetentionPeriodIsNegative() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("shard-1", -1).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Retention period must be greater than zero", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogVolumeNotOpen() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("non-existent-volume", 24).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid volume name: 'non-existent-volume'", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogWithInvalidNumberOfArguments() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$12\r\nVOLUME.ADMIN\r\n$15\r\nPRUNE-CHANGELOG\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'VOLUME.ADMIN PRUNE-CHANGELOG' command", actualMessage.content());
    }
}
