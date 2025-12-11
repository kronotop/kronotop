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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.commandbuilder.kronotop.VolumeAdminCommandBuilder;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeNames;
import com.kronotop.volume.VolumeSession;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;

import static com.kronotop.cluster.sharding.ShardStatus.READONLY;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationServiceTest extends BaseNetworkedVolumeIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationServiceTest.class);

    private void appendEntries(int number, int length) throws IOException {
        ByteBuffer[] entries = getEntries(number, length);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }
        assertEquals(number, result.getVersionstampedKeys().length);
    }

    private void setStandbyMember(KronotopInstance standby) {
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

    private void unsetStandbyMember(KronotopInstance standby) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.route("UNSET", "STANDBY", SHARD_KIND.name(), SHARD_ID, standby.getMember().getId()).encode(buf);

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
            return !route.standbys().contains(standby.getMember());
        });
    }

    private void setPrimaryOwner(KronotopInstance standby) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setShardStatus(SHARD_KIND.name(), SHARD_ID, READONLY.name()).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            VolumeAdminCommandBuilder<String, String> cmd2 = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            cmd2.setStatus(VolumeNames.format(SHARD_KIND, SHARD_ID), READONLY.name()).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            ByteBuf buf = Unpooled.buffer();
            cmd.route("SET", "PRIMARY", SHARD_KIND.name(), SHARD_ID, standby.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            if (msg instanceof SimpleStringRedisMessage actualMessage) {
                return Response.OK.equals(actualMessage.content());
            } else {
                assertInstanceOf(ErrorRedisMessage.class, msg);
                ErrorRedisMessage err = (ErrorRedisMessage) msg;
                LOGGER.error("setPrimaryOwner failed: {}", err.content());
            }
            return false;
        });

        RoutingService routing = standby.getContext().getService(RoutingService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Route route = routing.findRoute(SHARD_KIND, SHARD_ID);
            if (route == null) {
                return false;
            }
            return route.primary().equals(standby.getMember());
        });
    }

    @Test
    void shouldStartReplicationForShardWhenStandbyMemberIsAssigned() throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        replicationService.start();

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(ShardKind.REDIS);
            if (shardIds != null && shardIds.size() == 1) {
                return shardIds.getFirst() == 1;
            }
            return false;
        });
    }

    @Test
    void shouldStopReplicationWhenStopReplicationIsCalled() {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        replicationService.start();

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            if (shardIds != null && shardIds.size() == 1) {
                return shardIds.getFirst() == 1;
            }
            return false;
        });

        assertDoesNotThrow(() -> replicationService.stopReplication(SHARD_KIND, SHARD_ID, false));
        assertTrue(replicationService.listActiveReplicationsByShard().isEmpty());
    }

    @Test
    void shouldTriggerReplicationAutomaticallyViaCreateReplicationSlotHook() throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(ShardKind.REDIS);
            if (shardIds != null && shardIds.size() == 1) {
                return shardIds.getFirst() == 1;
            }
            return false;
        });
    }

    @Test
    void shouldStopReplicationWhenStandbyMemberIsUnset() {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            if (shardIds != null && shardIds.size() == 1) {
                return shardIds.getFirst() == 1;
            }
            return false;
        });

        unsetStandbyMember(standby);

        await().atMost(Duration.ofSeconds(15)).until(() -> replicationService.listActiveReplicationsByShard().isEmpty());
    }

    @Test
    void shouldStopReplicationWhenStandbyIsPromotedToPrimary() throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(ShardKind.REDIS);
            if (shardIds != null && shardIds.size() == 1) {
                return shardIds.getFirst() == 1;
            }
            return false;
        });

        setPrimaryOwner(standby);
        await().atMost(Duration.ofSeconds(15)).until(() -> replicationService.listActiveReplicationsByShard().isEmpty());
    }

    @Test
    void shouldReconnectToNewPrimaryWhenPrimaryOwnerChanges() throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        // S1: first standby
        KronotopInstance standby1 = addNewInstance();
        setStandbyMember(standby1);

        ReplicationService replicationService1 = standby1.getContext().getService(ReplicationService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService1.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });

        // S2: second standby
        KronotopInstance standby2 = addNewInstance();
        setStandbyMember(standby2);

        ReplicationService replicationService2 = standby2.getContext().getService(ReplicationService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService2.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });

        // Promote S2 to primary
        setPrimaryOwner(standby2);

        // S2 should stop replication (it's now the primary)
        await().atMost(Duration.ofSeconds(15)).until(() ->
                replicationService2.listActiveReplicationsByShard().isEmpty()
        );

        // S1 should reconnect and still have active replication
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService1.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });
    }

    @Test
    void shouldNotAutoRestartReplicationWhenStoppedWithSuppressionEnabled() {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Wait for replication to start automatically via CreateReplicationSlotHook
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });

        // Stop replication with suppression enabled
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, true);
        assertTrue(replicationService.listActiveReplicationsByShard().isEmpty());

        // Attempt to auto-start replication (non-explicit) - should be blocked by suppression
        // This should return early without starting replication
        assertDoesNotThrow(() -> replicationService.startReplication(SHARD_KIND, SHARD_ID, false));

        // Verify replication is still not running due to suppression
        assertTrue(replicationService.listActiveReplicationsByShard().isEmpty());
    }

    @Test
    void shouldRestartReplicationWhenExplicitlyStartedAfterSuppression() {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Wait for replication to start automatically via CreateReplicationSlotHook
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });

        // Stop replication with suppression enabled
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, true);
        assertTrue(replicationService.listActiveReplicationsByShard().isEmpty());

        // Explicit start should clear suppression and restart replication
        assertDoesNotThrow(() -> replicationService.startReplication(SHARD_KIND, SHARD_ID, true));

        // Verify replication is running again
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });
    }

    @Test
    void shouldAutoRestartReplicationWhenStoppedWithoutSuppression() {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Wait for replication to start automatically via CreateReplicationSlotHook
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });

        // Stop replication without suppression
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, false);
        assertTrue(replicationService.listActiveReplicationsByShard().isEmpty());

        // Non-explicit start should work since suppression is not enabled
        assertDoesNotThrow(() -> replicationService.startReplication(SHARD_KIND, SHARD_ID, false));

        // Verify replication is running again
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });
    }

    @Test
    void shouldPopulateReplicationStatusInfoDuringReplication() throws IOException {
        // Append enough entries to trigger replication
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Wait for replication to start
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });

        // Wait until stage is set (either SEGMENT_REPLICATION or CHANGE_DATA_CAPTURE)
        String volumeName = VolumeNames.format(SHARD_KIND, SHARD_ID);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = standby.getContext().getFoundationDB().createTransaction()) {
                DirectorySubspace standbySubspace = ReplicationUtil.openStandbySubspace(
                        standby.getContext(), tr, volumeName, standby.getMember().getId());
                ReplicationStatusInfo status = ReplicationUtil.readReplicationStatusInfo(tr, standbySubspace);
                return status.stage() != null;
            }
        });

        // Verify status info fields are populated
        try (Transaction tr = standby.getContext().getFoundationDB().createTransaction()) {
            DirectorySubspace standbySubspace = ReplicationUtil.openStandbySubspace(
                    standby.getContext(), tr, volumeName, standby.getMember().getId());
            ReplicationStatusInfo status = ReplicationUtil.readReplicationStatusInfo(tr, standbySubspace);

            assertNotNull(status.stage());
            assertNotNull(status.cursor());
            assertNotNull(status.status());
            assertNull(status.errorMessage());
            assertNotNull(status.cdcStageInfo());
            assertNotNull(status.segmentReplicationInfo());
        }
    }
}