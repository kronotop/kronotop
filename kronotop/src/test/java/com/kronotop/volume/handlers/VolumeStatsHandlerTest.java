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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseTest;
import com.kronotop.commands.VolumeStatsCommandBuilder;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VolumeStatsHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    private static final long SEGMENT_SIZE = 1_048_576L;

    private Map<String, Long> executeOpcounters() {
        VolumeStatsCommandBuilder<String, String> cmd = new VolumeStatsCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.opcounters(volumeConfig.name()).encode(buf);

        Object raw = BaseTest.runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, raw);
        MapRedisMessage mapMessage = (MapRedisMessage) raw;

        Map<String, Long> result = new HashMap<>();
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) entry.getKey();
            IntegerRedisMessage value = (IntegerRedisMessage) entry.getValue();
            result.put(key.content().toString(StandardCharsets.UTF_8), value.value());
        }
        return result;
    }

    private List<Map<String, Object>> executeSegments() {
        VolumeStatsCommandBuilder<String, String> cmd = new VolumeStatsCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.segments(volumeConfig.name()).encode(buf);

        Object raw = BaseTest.runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, raw);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) raw;

        List<Map<String, Object>> result = new ArrayList<>();
        for (RedisMessage element : arrayMessage.children()) {
            assertInstanceOf(MapRedisMessage.class, element);
            MapRedisMessage mapMessage = (MapRedisMessage) element;

            Map<String, Object> segment = new HashMap<>();
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                String key = ((FullBulkStringRedisMessage) entry.getKey()).content().toString(StandardCharsets.UTF_8);
                RedisMessage value = entry.getValue();
                if (value instanceof IntegerRedisMessage intMsg) {
                    segment.put(key, intMsg.value());
                } else if (value instanceof DoubleRedisMessage dblMsg) {
                    segment.put(key, dblMsg.value());
                }
            }
            result.add(segment);
        }
        return result;
    }

    @Test
    void shouldReturnZeroCountersForFreshVolume() {
        // Behavior: A freshly created volume with no operations returns all zero counters.
        Map<String, Long> counters = executeOpcounters();

        assertEquals(7, counters.size());
        assertEquals(0L, counters.get("appends"));
        assertEquals(0L, counters.get("deletes"));
        assertEquals(0L, counters.get("updates"));
        assertEquals(0L, counters.get("gets"));
        assertEquals(0L, counters.get("bytes_appended"));
        assertEquals(0L, counters.get("bytes_read"));
        assertEquals(0L, counters.get("segments_created"));
    }

    @Test
    void shouldCountAppendsAndBytesAppended() throws IOException {
        // Behavior: Appending entries increments appends by the number of entries and bytes_appended by total bytes.
        // The default segment is reused, so segments_created stays zero.
        ByteBuffer[] entries1 = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries1);
            tr.commit().join();
        }

        ByteBuffer[] entries2 = baseVolumeTestWrapper.getEntries(5);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries2);
            tr.commit().join();
        }

        Map<String, Long> counters = executeOpcounters();

        assertEquals(15L, counters.get("appends"));
        assertEquals(150L, counters.get("bytes_appended"));
        assertEquals(0L, counters.get("segments_created"));
        assertEquals(0L, counters.get("deletes"));
        assertEquals(0L, counters.get("updates"));
        assertEquals(0L, counters.get("gets"));
        assertEquals(0L, counters.get("bytes_read"));
    }

    @Test
    void shouldCountGetsAndBytesRead() throws IOException {
        // Behavior: Reading entries increments gets by 1 per call and bytes_read by the entry size.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        Versionstamp[] keys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        for (int i = 0; i < 3; i++) {
            VolumeSession session = new VolumeSession(prefix);
            volume.get(session, keys[i]);
        }

        Map<String, Long> counters = executeOpcounters();

        assertEquals(3L, counters.get("gets"));
        assertEquals(30L, counters.get("bytes_read"));
        assertEquals(5L, counters.get("appends"));
        assertEquals(50L, counters.get("bytes_appended"));
        assertEquals(0L, counters.get("deletes"));
        assertEquals(0L, counters.get("updates"));
    }

    @Test
    void shouldCountDeletes() throws IOException {
        // Behavior: Deleting entries increments deletes by the number of deleted entries across all calls.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        Versionstamp[] keys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.delete(session, keys[0], keys[1]);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.delete(session, keys[2]);
            tr.commit().join();
        }

        Map<String, Long> counters = executeOpcounters();

        assertEquals(3L, counters.get("deletes"));
        assertEquals(5L, counters.get("appends"));
        assertEquals(50L, counters.get("bytes_appended"));
        assertEquals(0L, counters.get("updates"));
        assertEquals(0L, counters.get("gets"));
        assertEquals(0L, counters.get("bytes_read"));
    }

    @Test
    void shouldCountUpdates() throws IOException, KeyNotFoundException {
        // Behavior: Updating entries increments updates by the number of updated entries and adds updated entry bytes to bytes_appended.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(3);
        Versionstamp[] keys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        ByteBuffer[] updateEntries = getEntries(2, 20);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.update(session,
                    new KeyEntry(keys[0], updateEntries[0]),
                    new KeyEntry(keys[1], updateEntries[1])
            );
            tr.commit().join();
        }

        Map<String, Long> counters = executeOpcounters();

        assertEquals(2L, counters.get("updates"));
        assertEquals(3L, counters.get("appends"));
        assertEquals(70L, counters.get("bytes_appended")); // 30 from append + 40 from update
        assertEquals(0L, counters.get("deletes"));
        assertEquals(0L, counters.get("gets"));
        assertEquals(0L, counters.get("bytes_read"));
    }

    // --- SEGMENTS subcommand tests ---

    @Test
    void shouldResetCounters() throws IOException {
        // Behavior: After reset, all counters return to zero.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Verify non-zero counters before reset
        Map<String, Long> before = executeOpcounters();
        assertTrue(before.get("appends") > 0);

        // Reset
        VolumeStatsCommandBuilder<String, String> cmd = new VolumeStatsCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.reset(volumeConfig.name()).encode(buf);
        Object raw = BaseTest.runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, raw);

        // Verify all counters are zero after reset
        Map<String, Long> after = executeOpcounters();

        assertEquals(0L, after.get("appends"));
        assertEquals(0L, after.get("deletes"));
        assertEquals(0L, after.get("updates"));
        assertEquals(0L, after.get("gets"));
        assertEquals(0L, after.get("bytes_appended"));
        assertEquals(0L, after.get("bytes_read"));
        assertEquals(0L, after.get("segments_created"));
    }

    @Test
    void shouldReturnDefaultSegmentForFreshVolume() {
        // Behavior: A freshly created volume with no operations returns one default segment with zero usage.
        List<Map<String, Object>> segments = executeSegments();
        assertEquals(1, segments.size());

        Map<String, Object> seg = segments.getFirst();
        assertEquals(SEGMENT_SIZE, seg.get("size_bytes"));
        assertEquals(0L, seg.get("used_bytes"));
        assertEquals(SEGMENT_SIZE, seg.get("free_bytes"));
        assertEquals(0L, seg.get("garbage_bytes"));
        assertEquals(0.0, (double) seg.get("garbage_percentage"), 0.001);
        assertEquals(0L, seg.get("cardinality"));
    }

    @Test
    void shouldReturnCorrectSegmentStatsAfterAppend() throws IOException {
        // Behavior: Appending entries produces one segment with exact used_bytes, free_bytes, zero garbage, and correct cardinality.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        List<Map<String, Object>> segments = executeSegments();
        assertEquals(1, segments.size());

        Map<String, Object> seg = segments.getFirst();
        assertEquals(SEGMENT_SIZE, seg.get("size_bytes"));
        assertEquals(100L, seg.get("used_bytes"));
        assertEquals(SEGMENT_SIZE - 100L, seg.get("free_bytes"));
        assertEquals(0L, seg.get("garbage_bytes"));
        assertEquals(0.0, (double) seg.get("garbage_percentage"), 0.001);
        assertEquals(10L, seg.get("cardinality"));
    }

    @Test
    void shouldReflectGarbageAfterDelete() throws IOException {
        // Behavior: Deleting entries reduces used_bytes and cardinality, creates garbage_bytes, but free_bytes stays the same.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        Versionstamp[] keys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.delete(session, keys[0], keys[1]);
            tr.commit().join();
        }

        List<Map<String, Object>> segments = executeSegments();
        assertEquals(1, segments.size());

        Map<String, Object> seg = segments.getFirst();
        assertEquals(30L, seg.get("used_bytes"));
        assertEquals(SEGMENT_SIZE - 50L, seg.get("free_bytes"));
        assertEquals(20L, seg.get("garbage_bytes"));
        float expectedRatio = (float) (100 * 20) / SEGMENT_SIZE;
        assertEquals(expectedRatio, (double) seg.get("garbage_percentage"), 0.001);
        assertEquals(3L, seg.get("cardinality"));
    }

    @Test
    void shouldMatchOpcountersBytesAppendedAsGarbageAfterDeletingAll() throws IOException {
        // Behavior: After deleting all entries, garbage_bytes equals OPCOUNTERS bytes_appended and used_bytes is zero.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        Versionstamp[] keys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.delete(session, keys);
            tr.commit().join();
        }

        Map<String, Long> counters = executeOpcounters();
        List<Map<String, Object>> segments = executeSegments();
        assertEquals(1, segments.size());

        Map<String, Object> seg = segments.getFirst();
        assertEquals(0L, seg.get("used_bytes"));
        assertEquals(0L, seg.get("cardinality"));
        assertEquals(counters.get("bytes_appended"), seg.get("garbage_bytes"));
        assertEquals(SEGMENT_SIZE - 100L, seg.get("free_bytes"));
    }

    @Test
    void shouldReflectUpdateInSegmentStats() throws IOException, KeyNotFoundException {
        // Behavior: Updating entries adjusts used_bytes for the size difference, old data becomes garbage, cardinality unchanged.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(3);
        Versionstamp[] keys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        ByteBuffer[] updateEntries = getEntries(2, 20);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.update(session,
                    new KeyEntry(keys[0], updateEntries[0]),
                    new KeyEntry(keys[1], updateEntries[1])
            );
            tr.commit().join();
        }

        List<Map<String, Object>> segments = executeSegments();
        assertEquals(1, segments.size());

        Map<String, Object> seg = segments.getFirst();
        assertEquals(3L, seg.get("cardinality"));
        // 1 original (10) + 2 updated (20 each) = 50
        assertEquals(50L, seg.get("used_bytes"));
        // 30 from initial append + 40 from update writes = 70 total bytes written
        assertEquals(SEGMENT_SIZE - 70L, seg.get("free_bytes"));
        // 70 written - 50 used = 20 garbage (the 2 original 10-byte entries)
        assertEquals(20L, seg.get("garbage_bytes"));
    }

    @Test
    void shouldCrossCheckOpcountersAfterMixedOperations() throws IOException, KeyNotFoundException {
        // Behavior: After append + delete + update, OPCOUNTERS bytes_appended equals total bytes written to segment,
        // and garbage_bytes is internally consistent with the segment invariant.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        Versionstamp[] keys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        // Delete 1 entry (10 bytes become garbage)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.delete(session, keys[0]);
            tr.commit().join();
        }

        // Update 1 entry with a 20-byte entry (20 bytes written, old 10 bytes become garbage)
        ByteBuffer[] updateEntries = getEntries(1, 20);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.update(session, new KeyEntry(keys[1], updateEntries[0]));
            tr.commit().join();
        }

        Map<String, Long> counters = executeOpcounters();
        // 50 from append + 20 from update = 70
        assertEquals(70L, counters.get("bytes_appended"));

        List<Map<String, Object>> segments = executeSegments();
        assertEquals(1, segments.size());

        Map<String, Object> seg = segments.getFirst();
        assertEquals(SEGMENT_SIZE - 70L, seg.get("free_bytes"));
        // 3 untouched (10 each) + 1 updated (20) = 50
        assertEquals(50L, seg.get("used_bytes"));
        // 70 written - 50 used = 20 (1 deleted 10-byte + 1 old 10-byte from update)
        assertEquals(20L, seg.get("garbage_bytes"));

        // Invariant: size - free_bytes == used_bytes + garbage_bytes
        long sizeBytes = (long) seg.get("size_bytes");
        long freeBytes = (long) seg.get("free_bytes");
        long usedBytes = (long) seg.get("used_bytes");
        long garbageBytes = (long) seg.get("garbage_bytes");
        assertEquals(sizeBytes - freeBytes, usedBytes + garbageBytes);
    }

    // --- OVERVIEW subcommand tests ---

    private Map<String, Object> executeOverview() {
        VolumeStatsCommandBuilder<String, String> cmd = new VolumeStatsCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.overview(volumeConfig.name()).encode(buf);

        Object raw = BaseTest.runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, raw);
        MapRedisMessage mapMessage = (MapRedisMessage) raw;

        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
            String key = ((FullBulkStringRedisMessage) entry.getKey()).content().toString(StandardCharsets.UTF_8);
            RedisMessage value = entry.getValue();
            if (value instanceof FullBulkStringRedisMessage strMsg) {
                result.put(key, strMsg.content().toString(StandardCharsets.UTF_8));
            } else if (value instanceof IntegerRedisMessage intMsg) {
                result.put(key, intMsg.value());
            } else if (value instanceof DoubleRedisMessage dblMsg) {
                result.put(key, dblMsg.value());
            }
        }
        return result;
    }

    @Test
    void shouldReturnDefaultSegmentOverviewForFreshVolume() {
        // Behavior: A freshly created volume with no operations returns READWRITE status, one default segment with full free space, zero usage and zero cardinality.
        Map<String, Object> overview = executeOverview();

        assertEquals(9, overview.size());
        assertEquals("READWRITE", overview.get("status"));
        assertEquals(1L, overview.get("segment_count"));
        assertEquals(SEGMENT_SIZE, overview.get("total_size_bytes"));
        assertEquals(0L, overview.get("used_bytes"));
        assertEquals(SEGMENT_SIZE, overview.get("free_bytes"));
        assertEquals(0L, overview.get("garbage_bytes"));
        assertEquals(0.0, overview.get("garbage_percentage"));
        assertEquals(0.0, overview.get("fill_ratio"));
        assertEquals(0L, overview.get("total_cardinality"));
    }

    @Test
    void shouldReturnCorrectOverviewAfterAppend() throws IOException {
        // Behavior: Appending 10 entries (10 bytes each) produces one segment with correct aggregated stats and zero garbage.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        Map<String, Object> overview = executeOverview();

        assertEquals("READWRITE", overview.get("status"));
        assertEquals(1L, overview.get("segment_count"));
        assertEquals(SEGMENT_SIZE, overview.get("total_size_bytes"));
        assertEquals(100L, overview.get("used_bytes"));
        assertEquals(SEGMENT_SIZE - 100L, overview.get("free_bytes"));
        assertEquals(0L, overview.get("garbage_bytes"));
        assertEquals(0.0, overview.get("garbage_percentage"));
        assertEquals(100.0 / SEGMENT_SIZE, (double) overview.get("fill_ratio"), 0.0001);
        assertEquals(10L, overview.get("total_cardinality"));
    }

    @Test
    void shouldReflectGarbageInOverviewAfterDelete() throws IOException {
        // Behavior: Deleting entries from a single segment increases garbage_bytes, decreases used_bytes and cardinality, while free_bytes stays unchanged.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        Versionstamp[] keys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.delete(session, keys[0], keys[1]);
            tr.commit().join();
        }

        Map<String, Object> overview = executeOverview();

        assertEquals(1L, overview.get("segment_count"));
        assertEquals(SEGMENT_SIZE, overview.get("total_size_bytes"));
        assertEquals(30L, overview.get("used_bytes"));
        assertEquals(SEGMENT_SIZE - 50L, overview.get("free_bytes"));
        assertEquals(20L, overview.get("garbage_bytes"));
        assertEquals(20.0 / SEGMENT_SIZE, (double) overview.get("garbage_percentage"), 0.0001);
        assertEquals(50.0 / SEGMENT_SIZE, (double) overview.get("fill_ratio"), 0.0001);
        assertEquals(3L, overview.get("total_cardinality"));
    }

    @Test
    void shouldAggregateOverviewAcrossMultipleSegments() throws IOException {
        // Behavior: When entries span multiple segments, the overview aggregates total_size_bytes, used_bytes, free_bytes, garbage_bytes, and cardinality across all segments.
        int entrySize = 100480;
        ByteBuffer[] entries = getEntries(20, entrySize);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        Map<String, Object> overview = executeOverview();

        assertEquals(2L, overview.get("segment_count"));
        assertEquals(2 * SEGMENT_SIZE, overview.get("total_size_bytes"));
        assertEquals(20L * entrySize, overview.get("used_bytes"));
        assertEquals(2 * (SEGMENT_SIZE - 10L * entrySize), overview.get("free_bytes"));
        assertEquals(0L, overview.get("garbage_bytes"));
        assertEquals(0.0, overview.get("garbage_percentage"));
        assertEquals((20.0 * entrySize) / (2 * SEGMENT_SIZE), (double) overview.get("fill_ratio"), 0.0001);
        assertEquals(20L, overview.get("total_cardinality"));

        // Invariant: total_size - free_bytes == used_bytes + garbage_bytes
        long totalSize = (long) overview.get("total_size_bytes");
        long freeBytes = (long) overview.get("free_bytes");
        long usedBytes = (long) overview.get("used_bytes");
        long garbageBytes = (long) overview.get("garbage_bytes");
        assertEquals(totalSize - freeBytes, usedBytes + garbageBytes);
    }

    @Test
    void shouldReflectUpdateInOverview() throws IOException, KeyNotFoundException {
        // Behavior: Updating entries creates garbage from old data while maintaining correct used_bytes and cardinality in the overview.
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(3);
        Versionstamp[] keys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        ByteBuffer[] updateEntries = getEntries(2, 20);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.update(session,
                    new KeyEntry(keys[0], updateEntries[0]),
                    new KeyEntry(keys[1], updateEntries[1])
            );
            tr.commit().join();
        }

        Map<String, Object> overview = executeOverview();

        assertEquals(1L, overview.get("segment_count"));
        assertEquals(SEGMENT_SIZE, overview.get("total_size_bytes"));
        assertEquals(50L, overview.get("used_bytes")); // 1*10 + 2*20
        assertEquals(SEGMENT_SIZE - 70L, overview.get("free_bytes")); // 30 initial + 40 update
        assertEquals(20L, overview.get("garbage_bytes")); // 2 old 10-byte entries
        assertEquals(20.0 / SEGMENT_SIZE, (double) overview.get("garbage_percentage"), 0.0001);
        assertEquals(70.0 / SEGMENT_SIZE, (double) overview.get("fill_ratio"), 0.0001);
        assertEquals(3L, overview.get("total_cardinality"));

        // Invariant: total_size - free_bytes == used_bytes + garbage_bytes
        long totalSize = (long) overview.get("total_size_bytes");
        long freeBytes = (long) overview.get("free_bytes");
        long usedBytes = (long) overview.get("used_bytes");
        long garbageBytes = (long) overview.get("garbage_bytes");
        assertEquals(totalSize - freeBytes, usedBytes + garbageBytes);
    }

}
