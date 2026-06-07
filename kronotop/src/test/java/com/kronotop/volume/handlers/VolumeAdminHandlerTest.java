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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.net.HostAndPort;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.commands.TaskAdminCommandBuilder;
import com.kronotop.commands.VolumeAdminCommandBuilder;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.network.Address;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.*;
import com.kronotop.volume.replication.ReplicationService;
import com.kronotop.volume.segment.SegmentUtil;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VolumeAdminHandlerTest extends BaseNetworkedVolumeIntegrationTest {
    private final String volumeName = VolumeNames.format(SHARD_KIND, SHARD_ID);

    private void createStalePrefixes(int count) {
        String clusterName = context.getConfig().getString("cluster.name");
        List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).extend("test-stale-prefixes");
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = context.getDirectoryLayer().createOrOpen(tr, subpath).join();
            byte[] prefixPointer = subspace.pack("stale-pointer");
            for (int i = 0; i < count; i++) {
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtil.register(context, tr, prefixPointer, testPrefix);
            }
            // Clear the pointer to make all prefixes stale
            tr.clear(prefixPointer);
            tr.commit().join();
        }
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
        cmd.describe("stash-shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        actualMessage.children().forEach((k, v) -> {
            String key = ((FullBulkStringRedisMessage) k).content().toString(StandardCharsets.UTF_8);
            switch (key) {
                case "name":
                    assertEquals("stash-shard-1", ((FullBulkStringRedisMessage) v).content().toString(StandardCharsets.UTF_8));
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
                                case "size":
                                    IntegerRedisMessage size = (IntegerRedisMessage) v2;
                                    assertTrue(size.value() > 0);
                                    break;
                                case "free_bytes", "used_bytes", "cardinality":
                                    IntegerRedisMessage freeBytes = (IntegerRedisMessage) v2;
                                    assertTrue(freeBytes.value() > 0);
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
        cmd.listSegments("stash-shard-1").encode(buf);

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
        cmd.listSegments("stash-shard-1").encode(buf);

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
            cmd.setStatus("stash-shard-1", "READONLY").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.describe("stash-shard-1").encode(buf);

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
    void shouldReturnErrorWhenSetStatusWithInvalidNumberOfParameters() {
        // Send command without status parameter
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$12\r\nVOLUME.ADMIN\r\n$10\r\nSET-STATUS\r\n$13\r\nstash-shard-1\r\n".getBytes());

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
        cmd.setStatus("stash-shard-1", "INVALID").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Invalid volume status: INVALID"));
    }

    @Test
    void shouldCleanupOrphanFiles() throws IOException {
        ByteBuffer[] entries = getEntries(3, 10);

        VolumeService service = context.getService(VolumeService.NAME);
        Volume shard = service.findVolume("stash-shard-1");
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            shard.append(session, entries);
            tr.commit().join();
        }
        File orphanFile = new File(String.valueOf(Paths.get(shard.getConfig().dataDir(), SegmentUtil.DIRECTORY, "orphan-file")));
        assertTrue(orphanFile.createNewFile());

        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.cleanupOrphanFiles("stash-shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        RedisMessage message = actualMessage.children().getFirst();
        FullBulkStringRedisMessage path = (FullBulkStringRedisMessage) message;
        assertEquals(orphanFile.getAbsolutePath(), path.content().toString(StandardCharsets.UTF_8));
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
                FullBulkStringRedisMessage taskName = (FullBulkStringRedisMessage) message;
                if (MarkStalePrefixesTask.NAME.equals(taskName.content().toString(StandardCharsets.UTF_8))) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    void shouldReturnErrorWhenMarkStalePrefixesTaskAlreadyExists() {
        // Behavior: Starting a mark-stale-prefixes task while one is already running returns an error.

        // Create stale prefixes so the task has real work and doesn't finish instantly.
        createStalePrefixes(500);

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

        // Cleanup: stop the running task
        ByteBuf stopBuf = Unpooled.buffer();
        cmd.markStalePrefixes("STOP").encode(stopBuf);
        runCommand(channel, stopBuf);
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
            FullBulkStringRedisMessage taskName = (FullBulkStringRedisMessage) message;
            if (MarkStalePrefixesTask.NAME.equals(taskName.content().toString(StandardCharsets.UTF_8))) {
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
    void shouldLocateMarkStalePrefixesTask() {
        // Behavior: LOCATE returns a map with member_id and process_id of the task owner.
        createStalePrefixes(500);

        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        // Start the task
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("START").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Locate the task
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("LOCATE").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapMsg = (MapRedisMessage) msg;

            boolean foundMemberId = false;
            boolean foundProcessId = false;
            boolean foundExternalAddress = false;
            boolean foundInternalAddress = false;
            for (var entry : mapMsg.children().entrySet()) {
                String key = ((FullBulkStringRedisMessage) entry.getKey()).content().toString(StandardCharsets.US_ASCII);
                if ("member_id".equals(key)) {
                    String value = ((FullBulkStringRedisMessage) entry.getValue()).content().toString(StandardCharsets.US_ASCII);
                    assertFalse(value.isEmpty());
                    assertEquals(context.getMember().getId(), value);
                    foundMemberId = true;
                } else if ("process_id".equals(key)) {
                    String value = ((FullBulkStringRedisMessage) entry.getValue()).content().toString(StandardCharsets.US_ASCII);
                    assertFalse(value.isEmpty());
                    assertEquals(context.getMember().getProcessId(), VersionstampUtil.base32HexDecode(value));
                    foundProcessId = true;
                } else if ("external_address".equals(key)) {
                    String value = ((FullBulkStringRedisMessage) entry.getValue()).content().toString(StandardCharsets.US_ASCII);
                    Address expected = context.getMember().getExternalAddress();
                    assertEquals(HostAndPort.fromParts(expected.getHost(), expected.getPort()).toString(), value);
                    foundExternalAddress = true;
                } else if ("internal_address".equals(key)) {
                    String value = ((FullBulkStringRedisMessage) entry.getValue()).content().toString(StandardCharsets.US_ASCII);
                    Address expected = context.getMember().getInternalAddress();
                    assertEquals(HostAndPort.fromParts(expected.getHost(), expected.getPort()).toString(), value);
                    foundInternalAddress = true;
                }
            }
            assertTrue(foundMemberId);
            assertTrue(foundProcessId);
            assertTrue(foundInternalAddress);
            assertTrue(foundExternalAddress);
        }

        // Stop the task
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.markStalePrefixes("STOP").encode(buf);
            runCommand(channel, buf);
        }
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
        cmd.startReplication(volumeName).encode(buf);

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
        cmd.startReplication(volumeName).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for stash-shard-1", actualMessage.content());
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
        assertEquals("ERR This node is not a standby for stash-shard-1", actualMessage.content());
    }

    @Test
    void shouldFailStartReplicationWithInvalidNumberOfParameters() {
        // Missing volume-name parameter
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$12\r\nVOLUME.ADMIN\r\n$11\r\nREPLICATION\r\n$5\r\nSTART\r\n".getBytes());

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
        cmd.stopReplication(volumeName).encode(buf);

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
        cmd.stopReplication(volumeName).encode(buf);

        Object msg = runCommand(standby.getChannel(), buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR This node is not a standby for stash-shard-1", actualMessage.content());
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
        assertEquals("ERR This node is not a standby for stash-shard-1", actualMessage.content());
    }

    @Test
    void shouldFailStopReplicationWithInvalidNumberOfParameters() {
        // Missing volume-name parameter
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$12\r\nVOLUME.ADMIN\r\n$11\r\nREPLICATION\r\n$4\r\nSTOP\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid number of parameters", actualMessage.content());
    }

    @Test
    void shouldPruneChangelogSuccessfully() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("stash-shard-1", 24).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogRetentionPeriodIsZero() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("stash-shard-1", 0).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR retention period must be greater than zero", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenPruneChangelogRetentionPeriodIsNegative() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.pruneChangelog("stash-shard-1", -1).encode(buf);

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