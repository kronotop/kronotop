/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.commands.VolumeInspectCommandBuilder;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeNames;
import com.kronotop.volume.VolumeSession;
import com.kronotop.volume.replication.ReplicationService;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VolumeInspectHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @BeforeEach
    void useRESP3() {
        // These tests assert RESP3 types, so the session must speak RESP3.
        switchProtocol(channel, RESPVersion.RESP3);
    }

    private void appendEntries(int number, int length) throws IOException {
        ByteBuffer[] entries = getEntries(number, length);
        AppendResult result;
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }
        assertEquals(number, result.getVersionstampedKeys().length);
    }

    private void setStandbyMember(KronotopInstance standby) {
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

    private MapRedisMessage inspectCursor() {
        VolumeInspectCommandBuilder<String, String> cmd = new VolumeInspectCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.cursor("shard-1").encode(buf);
        return assertInstanceOf(MapRedisMessage.class, runCommand(channel, buf));
    }

    private long integerField(MapRedisMessage cursor, String field) {
        return assertInstanceOf(IntegerRedisMessage.class, findInMapMessage(cursor, field)).value();
    }

    private String stringField(MapRedisMessage cursor, String field) {
        return stringValueOf(assertInstanceOf(FullBulkStringRedisMessage.class, findInMapMessage(cursor, field)));
    }

    @Test
    void shouldReturnCursorForVolumeWithData() throws IOException {
        // Behavior: Ten entries are appended to an empty volume and committed. VOLUME.INSPECT CURSOR returns 0
        // for active_segment_id and tail_segment_id, a non-empty versionstamp, a positive next_position and a
        // sequence_number of 0 or more.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        MapRedisMessage cursor = inspectCursor();

        assertEquals(0L, integerField(cursor, "active_segment_id"));
        assertEquals(0L, integerField(cursor, "tail_segment_id"));
        assertFalse(stringField(cursor, "versionstamp").isEmpty());
        assertTrue(integerField(cursor, "next_position") > 0);
        assertTrue(integerField(cursor, "sequence_number") >= 0);
    }

    @Test
    void shouldReturnCursorForEmptyVolume() {
        // Behavior: No entry has been appended to the volume. VOLUME.INSPECT CURSOR returns 0 for
        // active_segment_id and next_position, -1 for tail_segment_id and sequence_number, and an empty
        // versionstamp.
        MapRedisMessage cursor = inspectCursor();

        assertEquals(0L, integerField(cursor, "active_segment_id"));
        assertEquals(-1L, integerField(cursor, "tail_segment_id"));
        assertTrue(stringField(cursor, "versionstamp").isEmpty());
        assertEquals(0L, integerField(cursor, "next_position"));
        assertEquals(-1L, integerField(cursor, "sequence_number"));
    }

    @Test
    void shouldReturnTailSegmentCursorWhenActiveSegmentHasNoCommittedEntry() throws IOException {
        // Behavior: Segment 0 is full and its entries are committed. A second append creates segment 1, but its
        // transaction never commits. VOLUME.INSPECT CURSOR returns 1 for active_segment_id and 0 for
        // tail_segment_id. next_position is the segment size, versionstamp is not empty and sequence_number is
        // positive.
        fillSegmentThenRollOverWithoutCommit(1024);

        MapRedisMessage cursor = inspectCursor();

        assertEquals(1L, integerField(cursor, "active_segment_id"));
        assertEquals(0L, integerField(cursor, "tail_segment_id"));
        assertFalse(stringField(cursor, "versionstamp").isEmpty());
        assertEquals(volume.getConfig().segmentSize(), integerField(cursor, "next_position"));
        assertTrue(integerField(cursor, "sequence_number") > 0);
    }

    @Test
    void shouldReturnErrorWhenStandbyHasNoReplicationState() {
        // Behavior: The cluster has a single member, which is the primary of the shard and has never
        // replicated it as a standby. VOLUME.INSPECT REPLICATION for the shard volume with this member's
        // ID replies with the error "ERR No such standby member: '<member id>'".
        VolumeInspectCommandBuilder<String, String> cmd = new VolumeInspectCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        String memberId = context.getMember().getId();
        cmd.replication(VolumeNames.format(SHARD_ID), memberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals(String.format("ERR No such standby member: '%s'", memberId), errorMessage.content());
    }

    @Test
    void shouldReturnErrorWhenInspectReplicationWithInvalidVolumeName() {
        // Behavior: VOLUME.INSPECT REPLICATION is called with the volume name "volume-1", which does not
        // start with the "shard-" prefix, and the ID of an existing member. The command replies with the
        // error "ERR Invalid volume name: 'volume-1'".
        VolumeInspectCommandBuilder<String, String> cmd = new VolumeInspectCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.replication("volume-1", context.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid volume name: 'volume-1'", errorMessage.content());
    }

    @Test
    void shouldReturnErrorWhenInspectReplicationWithUnknownShard() {
        // Behavior: VOLUME.INSPECT REPLICATION is called with the well-formed volume name "shard-9999",
        // whose shard ID does not exist in the cluster, and the ID of an existing member. The command
        // replies with the error "ERR Invalid shard id: '9999'".
        VolumeInspectCommandBuilder<String, String> cmd = new VolumeInspectCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.replication("shard-9999", context.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid shard id: '9999'", errorMessage.content());
    }

    @Test
    void shouldReturnReplicationStatusForActiveReplication() throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Wait for replication to start
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            List<Integer> shardIds = replicationService.listActiveReplications();
            return shardIds.contains(SHARD_ID);
        });

        VolumeInspectCommandBuilder<String, String> cmd = new VolumeInspectCommandBuilder<>(StringCodec.ASCII);

        // Wait until stage is populated (replication state written to FDB)
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            ByteBuf b = Unpooled.buffer();
            cmd.replication(VolumeNames.format(SHARD_ID), standby.getMember().getId()).encode(b);
            Object m = runCommand(channel, b);
            if (m instanceof MapRedisMessage mapMsg) {
                for (var entry : mapMsg.children().entrySet()) {
                    String key = ((FullBulkStringRedisMessage) entry.getKey()).content().toString(StandardCharsets.UTF_8);
                    if ("stage".equals(key)) {
                        FullBulkStringRedisMessage v = (FullBulkStringRedisMessage) entry.getValue();
                        return !v.content().toString(StandardCharsets.UTF_8).isEmpty();
                    }
                }
            }
            return false;
        });

        // Query replication status
        ByteBuf buf = Unpooled.buffer();
        cmd.replication(VolumeNames.format(SHARD_ID), standby.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        for (var entry : actualMessage.children().entrySet()) {
            String key = ((FullBulkStringRedisMessage) entry.getKey()).content().toString(StandardCharsets.UTF_8);
            RedisMessage value = entry.getValue();
            switch (key) {
                case "stage", "status" -> {
                    FullBulkStringRedisMessage v = (FullBulkStringRedisMessage) value;
                    assertFalse(v.content().toString(StandardCharsets.UTF_8).isEmpty());
                }
                case "cursor", "segment_replication_stage", "cdc_stage" -> {
                    assertInstanceOf(MapRedisMessage.class, value);
                    MapRedisMessage cursorMap = (MapRedisMessage) value;
                    assertFalse(cursorMap.children().isEmpty());
                }
            }
        }
    }
}
