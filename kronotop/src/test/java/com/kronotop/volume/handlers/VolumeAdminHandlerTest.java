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
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.commandbuilder.kronotop.TaskAdminCommandBuilder;
import com.kronotop.commandbuilder.kronotop.VolumeAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.*;
import com.kronotop.volume.replication.ReplicationService;
import com.kronotop.volume.segment.Segment;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VolumeAdminHandlerTest extends BaseNetworkedVolumeIntegrationTest {

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
        cmd.describe("redis-shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        actualMessage.children().forEach((k, v) -> {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) k;
            switch (key.content()) {
                case "name":
                    assertEquals("redis-shard-1", ((SimpleStringRedisMessage) v).content());
                    break;
                case "status":
                    assertEquals(VolumeStatus.READWRITE.name(), ((SimpleStringRedisMessage) v).content());
                    break;
                case "data_dir":
                    SimpleStringRedisMessage dataDir = (SimpleStringRedisMessage) v;
                    assertFalse(dataDir.content().isEmpty());
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
                            String segmentKey = ((SimpleStringRedisMessage) k2).content();
                            switch (segmentKey) {
                                case "size":
                                    IntegerRedisMessage size = (IntegerRedisMessage) v2;
                                    assertTrue(size.value() > 0);
                                    break;
                                case "free_bytes", "used_bytes", "cardinality":
                                    IntegerRedisMessage freeBytes = (IntegerRedisMessage) v2;
                                    assertTrue(freeBytes.value() > 0);
                                    break;
                                case "garbage_ratio":
                                    // TODO: Waits for KR-9
                                    break;
                            }
                        });
                    });
            }
        });
    }

    @Test
    void shouldReturnErrorWhenDescribeVolumeWithInvalidNumberOfParameters() {
        // Send command without volume name parameter
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$12\r\nVOLUME.ADMIN\r\n$8\r\nDESCRIBE\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("invalid number of parameters"));
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
        cmd.listSegments("redis-shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(0, actualMessage.children().size());
    }

    @Test
    void shouldListSegments() throws IOException {
        injectTestData();
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.listSegments("redis-shard-1").encode(buf);

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
            cmd.setStatus("redis-shard-1", "READONLY").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.describe("redis-shard-1").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            actualMessage.children().forEach((k, v) -> {
                SimpleStringRedisMessage key = (SimpleStringRedisMessage) k;
                if (key.content().equals("status")) {
                    assertEquals(VolumeStatus.READONLY.name(), ((SimpleStringRedisMessage) v).content());
                }
            });
        }
    }

    @Test
    void shouldReturnErrorWhenSetStatusWithInvalidNumberOfParameters() {
        // Send command without status parameter
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$12\r\nVOLUME.ADMIN\r\n$10\r\nSET-STATUS\r\n$13\r\nredis-shard-1\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("invalid number of parameters"));
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
        cmd.setStatus("redis-shard-1", "INVALID").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Invalid volume status: INVALID"));
    }

    @Test
    void shouldStartVacuumTask() {
        String volumeName = "redis-shard-1";

        VolumeAdminCommandBuilder<String, String> volumeAdmin = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            volumeAdmin.vacuum(volumeName, 10.0).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
        {
            TaskAdminCommandBuilder<String, String> taskAdmin = new TaskAdminCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            taskAdmin.list().encode(buf);

            KronotopTestInstance instance = getInstances().getFirst();
            instance.getChannel().writeInbound(buf);
            Object msg = instance.getChannel().readOutbound();
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            boolean found = false;
            for (RedisMessage message : actualMessage.children().keySet()) {
                SimpleStringRedisMessage taskName = (SimpleStringRedisMessage) message;
                if (VacuumMetadata.VacuumTaskName(volumeName).equals(taskName.content())) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    void shouldStopVacuumTask() throws IOException {
        String volumeName = "redis-shard-1";

        // Inject data so vacuum has work to do and doesn't complete instantly
        injectTestData();

        VolumeAdminCommandBuilder<String, String> volumeAdmin = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            volumeAdmin.vacuum(volumeName, 10.0).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            volumeAdmin.stopVacuum(volumeName).encode(buf);

            Object msg = runCommand(channel, buf);
            // The vacuum task may complete before we can stop it (race condition).
            // Both outcomes are valid: either we stopped it, or it already completed.
            if (msg instanceof SimpleStringRedisMessage actualMessage) {
                assertEquals(Response.OK, actualMessage.content());
            } else if (msg instanceof ErrorRedisMessage actualMessage) {
                assertEquals("ERR Vacuum task not found on " + volumeName, actualMessage.content());
            } else {
                fail("Unexpected response type: " + msg.getClass());
            }
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertNull(VacuumMetadata.load(tr, volumeConfig.subspace()));
        }
    }

    @Test
    void shouldReturnErrorWhenStoppingNonExistentVacuumTask() {
        String volumeName = "redis-shard-1";

        VolumeAdminCommandBuilder<String, String> volumeAdmin = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        volumeAdmin.stopVacuum(volumeName).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Vacuum task not found on redis-shard-1", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenVacuumingNonOpenVolume() {
        VolumeAdminCommandBuilder<String, String> volumeAdmin = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        volumeAdmin.vacuum("volume-name", 10.0).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Volume: 'volume-name' is not open", actualMessage.content());
    }

    @Test
    void shouldCleanupOrphanFiles() throws IOException {
        ByteBuffer[] entries = getEntries(3, 10);

        VolumeService service = context.getService(VolumeService.NAME);
        Volume shard = service.findVolume("redis-shard-1");
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            shard.append(session, entries);
            tr.commit().join();
        }
        File orphanFile = new File(String.valueOf(Paths.get(shard.getConfig().dataDir(), Segment.SEGMENTS_DIRECTORY, "orphan-file")));
        assertTrue(orphanFile.createNewFile());

        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.cleanupOrphanFiles("redis-shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        RedisMessage message = actualMessage.children().getFirst();
        SimpleStringRedisMessage path = (SimpleStringRedisMessage) message;
        assertEquals(orphanFile.getAbsolutePath(), path.content());
        // Deleted
        assertFalse(orphanFile.exists());
    }

    @Test
    void shouldStartMarkStalePrefixesTask() {
        {
            VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("START").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            TaskAdminCommandBuilder<String, String> taskAdmin = new TaskAdminCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            taskAdmin.list().encode(buf);

            KronotopTestInstance instance = getInstances().getFirst();
            instance.getChannel().writeInbound(buf);
            Object msg = instance.getChannel().readOutbound();
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            boolean found = false;
            for (RedisMessage message : actualMessage.children().keySet()) {
                SimpleStringRedisMessage taskName = (SimpleStringRedisMessage) message;
                if (MarkStalePrefixesTask.NAME.equals(taskName.content())) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    void shouldReturnErrorWhenMarkStalePrefixesTaskAlreadyExists() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("START").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        ByteBuf buf = Unpooled.buffer();
        cmd.markStalePrefixes("START").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Task volume:mark-stale-prefixes-task already exists", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenStoppingNonExistentMarkStalePrefixesTask() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.markStalePrefixes("STOP").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Task with name volume:mark-stale-prefixes-task does not exist", actualMessage.content());
    }

    boolean hasMarkStalePrefixesTask() {
        TaskAdminCommandBuilder<String, String> taskAdmin = new TaskAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        taskAdmin.list().encode(buf);

        KronotopTestInstance instance = getInstances().getFirst();
        instance.getChannel().writeInbound(buf);
        Object msg = instance.getChannel().readOutbound();
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        for (RedisMessage message : actualMessage.children().keySet()) {
            SimpleStringRedisMessage taskName = (SimpleStringRedisMessage) message;
            if (MarkStalePrefixesTask.NAME.equals(taskName.content())) {
                return true;
            }
        }
        return false;
    }

    @Test
    void shouldStartThenStopMarkStalePrefixesTask() {
        {
            VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("START").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("STOP").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        assertFalse(hasMarkStalePrefixesTask());
    }

    @Test
    void shouldRemoveMarkStalePrefixesTask() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("START").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("REMOVE").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        assertFalse(hasMarkStalePrefixesTask());
    }

    @Test
    void shouldReturnErrorWhenRemovingNonExistentMarkStalePrefixesTask() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("REMOVE").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR Task with name volume:mark-stale-prefixes-task does not exist", actualMessage.content());
        }
    }

    private void setStandbyMember(KronotopTestInstance standby) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", SHARD_KIND.name(), SHARD_ID, standby.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        RoutingService routing = standby.getContext().getService(RoutingService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Route route = routing.findRoute(SHARD_KIND, SHARD_ID);
            if (route == null) {
                return false;
            }
            return route.standbys().contains(standby.getMember());
        });
    }

    private void stopReplication(KronotopTestInstance standby) {
        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, true);
    }

    @Test
    void shouldStartReplicationSuccessfully() {
        KronotopTestInstance standby = addNewInstance();
        setStandbyMember(standby);

        // Stop the auto-started replication first
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
            EnumMap<ShardKind, List<Integer>> active = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = active.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });
        stopReplication(standby);

        // Verify replication is stopped
        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        assertTrue(replicationService.listActiveReplicationsByShard().isEmpty());

        // Start replication via command
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.startReplication(SHARD_KIND.name(), SHARD_ID).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify replication has started
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> active = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = active.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });
    }

    @Test
    void shouldFailStartReplicationWhenNoRouteFound() {
        KronotopTestInstance standby = addNewInstance();

        // Try to start replication for a shard that has no route (standby not assigned)
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.startReplication(SHARD_KIND.name(), SHARD_ID).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for REDIS-1", actualMessage.content());
    }

    @Test
    void shouldFailStartReplicationWhenNodeIsNotStandby() {
        // Try to start replication on primary node (not a standby)
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.startReplication(SHARD_KIND.name(), SHARD_ID).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for REDIS-1", actualMessage.content());
    }

    @Test
    void shouldFailStartReplicationWithInvalidNumberOfParameters() {
        // Missing shard-id parameter
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$12\r\nVOLUME.ADMIN\r\n$17\r\nSTART-REPLICATION\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid number of parameters", actualMessage.content());
    }

    @Test
    void shouldStopReplicationSuccessfully() {
        KronotopTestInstance standby = addNewInstance();
        setStandbyMember(standby);

        // Wait for replication to start
        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> active = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = active.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });

        // Stop replication via command
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.stopReplication(SHARD_KIND.name(), SHARD_ID).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify replication has stopped
        assertTrue(replicationService.listActiveReplicationsByShard().isEmpty());
    }

    @Test
    void shouldFailStopReplicationWhenNoRouteFound() {
        KronotopTestInstance standby = addNewInstance();

        // Try to stop replication for a shard that has no route (standby not assigned)
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.stopReplication(SHARD_KIND.name(), SHARD_ID).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for REDIS-1", actualMessage.content());
    }

    @Test
    void shouldFailStopReplicationWhenNodeIsNotStandby() {
        // Try to stop replication on primary node (not a standby)
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.stopReplication(SHARD_KIND.name(), SHARD_ID).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for REDIS-1", actualMessage.content());
    }

    @Test
    void shouldFailStopReplicationWithInvalidNumberOfParameters() {
        // Missing shard-id parameter
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$12\r\nVOLUME.ADMIN\r\n$16\r\nSTOP-REPLICATION\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid number of parameters", actualMessage.content());
    }

    @Test
    void shouldPruneChangelogSuccessfully() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("redis-shard-1", 24).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogRetentionPeriodIsZero() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("redis-shard-1", 0).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR retention period must be greater than zero", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogRetentionPeriodIsNegative() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("redis-shard-1", -1).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR retention period must be greater than zero", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogVolumeNotOpen() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("non-existent-volume", 24).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid volume name: non-existent-volume", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogWithInvalidNumberOfParameters() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$12\r\nVOLUME.ADMIN\r\n$15\r\nPRUNE-CHANGELOG\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid number of parameters", actualMessage.content());
    }
}