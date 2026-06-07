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
import com.kronotop.BaseTest;
import com.kronotop.commands.VolumeStatsCommandBuilder;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeNames;
import com.kronotop.volume.replication.ReplicationState;
import com.kronotop.volume.replication.ReplicationStatus;
import com.kronotop.volume.replication.Stage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ReplicationStatsSubcommandTest extends BaseNetworkedVolumeIntegrationTest {

    private DirectorySubspace createStandbySubspace() {
        String volumeName = VolumeNames.format(SHARD_KIND, SHARD_ID);
        String standbyId = context.getMember().getId();
        KronotopDirectoryNode node = switch (SHARD_KIND) {
            case STASH ->
                    KronotopDirectory.kronotop().cluster(context.getClusterName()).metadata().volumes().stash().volume(volumeName).standby(standbyId);
            case BUCKET ->
                    KronotopDirectory.kronotop().cluster(context.getClusterName()).metadata().volumes().bucket().volume(volumeName).standby(standbyId);
        };
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = context.getDirectoryLayer().createOrOpen(tr, node.toList()).join();
            tr.commit().join();
            return subspace;
        }
    }

    private Map<String, Object> executeReplication() {
        String volumeName = VolumeNames.format(SHARD_KIND, SHARD_ID);
        String standbyId = context.getMember().getId();

        VolumeStatsCommandBuilder<String, String> cmd = new VolumeStatsCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.replication(volumeName, SHARD_KIND.name(), SHARD_ID, standbyId).encode(buf);

        Object raw = BaseTest.runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, raw);
        MapRedisMessage mapMessage = (MapRedisMessage) raw;

        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
            String key = ((FullBulkStringRedisMessage) entry.getKey()).content().toString(StandardCharsets.UTF_8);
            RedisMessage value = entry.getValue();
            if (value instanceof IntegerRedisMessage intMsg) {
                result.put(key, intMsg.value());
            } else if (value instanceof FullBulkStringRedisMessage bulkMsg) {
                result.put(key, bulkMsg.content().toString(StandardCharsets.UTF_8));
            }
        }
        return result;
    }

    @Test
    void shouldReturnDefaultValuesForEmptyStandbySubspace() {
        // Behavior: Subspace exists but no state written returns all defaults.
        createStandbySubspace();

        Map<String, Object> result = executeReplication();

        assertEquals(7, result.size());
        assertEquals("", result.get("stage"));
        assertEquals("WAITING", result.get("status"));
        assertEquals("", result.get("error_message"));
        assertEquals(0L, result.get("cursor_segment_id"));
        assertEquals(0L, result.get("cursor_position"));
        assertEquals(0L, result.get("cdc_sequence_number"));
        assertEquals(0L, result.get("cdc_position"));
    }

    @Test
    void shouldReturnSegmentReplicationStageValues() {
        // Behavior: Written SEGMENT_REPLICATION stage with position and RUNNING status reflects in response.
        DirectorySubspace subspace = createStandbySubspace();

        try (Transaction tr = database.createTransaction()) {
            ReplicationState.setStage(tr, subspace, 5, Stage.SEGMENT_REPLICATION);
            ReplicationState.setStatus(tr, subspace, 5, ReplicationStatus.RUNNING);
            ReplicationState.setPosition(tr, subspace, 5, 4096);
            tr.commit().join();
        }

        Map<String, Object> result = executeReplication();

        assertEquals("SEGMENT_REPLICATION", result.get("stage"));
        assertEquals("RUNNING", result.get("status"));
        assertEquals("", result.get("error_message"));
        assertEquals(5L, result.get("cursor_segment_id"));
        assertEquals(4096L, result.get("cursor_position"));
        assertEquals(0L, result.get("cdc_sequence_number"));
        assertEquals(4096L, result.get("cdc_position"));
    }

    @Test
    void shouldReturnChangeDataCaptureStageValues() {
        // Behavior: Written CDC stage with sequence number reflects in all CDC-specific fields.
        DirectorySubspace subspace = createStandbySubspace();

        try (Transaction tr = database.createTransaction()) {
            ReplicationState.setStage(tr, subspace, 3, Stage.CHANGE_DATA_CAPTURE);
            ReplicationState.setStatus(tr, subspace, 3, ReplicationStatus.DONE);
            ReplicationState.setPosition(tr, subspace, 3, 8192);
            ReplicationState.setSequenceNumber(tr, subspace, 3, 42);
            tr.commit().join();
        }

        Map<String, Object> result = executeReplication();

        assertEquals("CHANGE_DATA_CAPTURE", result.get("stage"));
        assertEquals("DONE", result.get("status"));
        assertEquals("", result.get("error_message"));
        assertEquals(3L, result.get("cursor_segment_id"));
        assertEquals(8192L, result.get("cursor_position"));
        assertEquals(42L, result.get("cdc_sequence_number"));
        assertEquals(8192L, result.get("cdc_position"));
    }

    @Test
    void shouldReturnErrorMessage() {
        // Behavior: Written FAILED status with error message appears in the response.
        DirectorySubspace subspace = createStandbySubspace();

        try (Transaction tr = database.createTransaction()) {
            ReplicationState.setStage(tr, subspace, 2, Stage.SEGMENT_REPLICATION);
            ReplicationState.setStatus(tr, subspace, 2, ReplicationStatus.FAILED);
            ReplicationState.setPosition(tr, subspace, 2, 1024);
            ReplicationState.setErrorMessage(tr, subspace, 2, "connection refused");
            tr.commit().join();
        }

        Map<String, Object> result = executeReplication();

        assertEquals("SEGMENT_REPLICATION", result.get("stage"));
        assertEquals("FAILED", result.get("status"));
        assertEquals("connection refused", result.get("error_message"));
        assertEquals(2L, result.get("cursor_segment_id"));
        assertEquals(1024L, result.get("cursor_position"));
        assertEquals(0L, result.get("cdc_sequence_number"));
        assertEquals(1024L, result.get("cdc_position"));
    }

    @Test
    void shouldReturnHighestSegmentCursorWhenMultipleSegmentsExist() {
        // Behavior: With multiple segments, readCursor picks the highest segmentId; all other fields read from that segment.
        DirectorySubspace subspace = createStandbySubspace();

        try (Transaction tr = database.createTransaction()) {
            // Segment 1
            ReplicationState.setPosition(tr, subspace, 1, 500);
            ReplicationState.setStage(tr, subspace, 1, Stage.CHANGE_DATA_CAPTURE);
            ReplicationState.setSequenceNumber(tr, subspace, 1, 10);

            // Segment 7
            ReplicationState.setPosition(tr, subspace, 7, 2048);
            ReplicationState.setStage(tr, subspace, 7, Stage.SEGMENT_REPLICATION);
            ReplicationState.setStatus(tr, subspace, 7, ReplicationStatus.RUNNING);
            ReplicationState.setSequenceNumber(tr, subspace, 7, 25);
            tr.commit().join();
        }

        Map<String, Object> result = executeReplication();

        assertEquals("SEGMENT_REPLICATION", result.get("stage"));
        assertEquals("RUNNING", result.get("status"));
        assertEquals("", result.get("error_message"));
        assertEquals(7L, result.get("cursor_segment_id"));
        assertEquals(2048L, result.get("cursor_position"));
        assertEquals(25L, result.get("cdc_sequence_number"));
        assertEquals(2048L, result.get("cdc_position"));
    }
}
