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
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commandbuilder.kronotop.TaskAdminCommandBuilder;
import com.kronotop.commandbuilder.kronotop.VolumeAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.*;
import com.kronotop.volume.replication.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.replication.ReplicationConfig;
import com.kronotop.volume.replication.ReplicationMetadata;
import com.kronotop.volume.replication.ReplicationStage;
import com.kronotop.volume.segment.Segment;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

import static com.kronotop.volume.VolumeTestUtils.getEntries;
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
    void test_volume_admin_list() {
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
    void test_volume_describe() throws IOException {
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
                        String name = ((SimpleStringRedisMessage) kk).content();
                        assertFalse(name.isEmpty());

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
    void test_volume_admin_set_status() {
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
    void test_volume_admin_vacuum() {
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
    void test_volume_admin_stop_vacuum() {
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
            ByteBuf buf = Unpooled.buffer();
            volumeAdmin.stopVacuum(volumeName).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertNull(VacuumMetadata.load(tr, volumeConfig.subspace()));
        }
    }

    @Test
    void test_volume_admin_stop_when_vacuum_task_not_found() {
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
    void test_volume_admin_vacuum_when_volume_not_open() {
        VolumeAdminCommandBuilder<String, String> volumeAdmin = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        volumeAdmin.vacuum("volume-name", 10.0).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Volume: 'volume-name' is not open", actualMessage.content());
    }

    @Test
    void test_volume_replications() {
        // TODO: We expose too much details to test this command.
        VolumeConfig volumeConfig = new VolumeConfigGenerator(context, ShardKind.REDIS, 1).volumeConfig();
        ReplicationConfig config = new ReplicationConfig(
                volumeConfig,
                ShardKind.REDIS,
                1,
                ReplicationStage.SNAPSHOT);
        Versionstamp replicationSlotId = ReplicationMetadata.newReplication(context, config);
        assertNotNull(replicationSlotId);

        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.replications().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        actualMessage.children().forEach((rawSlotId, slot) -> {
            String slotId = ((SimpleStringRedisMessage) rawSlotId).content();
            assertEquals(ReplicationMetadata.stringifySlotId(replicationSlotId), slotId);
            MapRedisMessage map = (MapRedisMessage) slot;
            map.children().forEach((key, value) -> {
                SimpleStringRedisMessage k = (SimpleStringRedisMessage) key;
                switch (k.content()) {
                    case "shard_kind" -> {
                        SimpleStringRedisMessage v = (SimpleStringRedisMessage) value;
                        assertEquals(ShardKind.REDIS.name(), v.content());
                    }
                    case "shard_id" -> {
                        IntegerRedisMessage v = (IntegerRedisMessage) value;
                        assertEquals(1, v.value());
                    }
                    case "replication_stage", "latest_versionstamped_key", "received_versionstamped_key" -> {
                        SimpleStringRedisMessage v = (SimpleStringRedisMessage) value;
                        assertEquals("", v.content());
                    }
                    case "latest_segment_id" -> {
                        IntegerRedisMessage v = (IntegerRedisMessage) value;
                        assertEquals(0, v.value());
                    }
                    case "active", "stale" -> {
                        BooleanRedisMessage v = (BooleanRedisMessage) value;
                        assertFalse(v.value());
                    }
                    case "completed_stages" -> {
                        ArrayRedisMessage v = (ArrayRedisMessage) value;
                        assertTrue(v.children().isEmpty());
                    }
                }
            });
        });
    }

    @Test
    void test_volume_cleanup_orphan_files() throws IOException {
        ByteBuffer[] entries = getEntries(3);

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
    void test_mark_stale_prefixes_start() {
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
    void test_mark_stale_prefixes_start_already_exists() {
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
    void test_mark_stale_prefixes_stop() {
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
    void test_mark_stale_prefixes_start_then_stop() {
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
    void test_mark_stale_prefixes_remove() {
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
    void test_mark_stale_prefixes_remove_not_found() {
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
}