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

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.cluster.client.protocol.ChangeLogRangeArgs;
import com.kronotop.cluster.client.protocol.InternalCommandBuilder;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ChangeLogRangeHandlerTest extends BaseNetworkedVolumeIntegrationTest {
    HybridLogicalClock hlc = new HybridLogicalClock();

    @Test
    void shouldReturnEntriesWithInclusiveRange() throws IOException {
        // Append entries to create changelog entries
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Query with inclusive range [0 10] using * for all operation kinds
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0"; // start from the beginning
        String end = String.format("%s]", hlc.next(System.currentTimeMillis()));
        cmd.changelogRange(volumeConfig.name(), "*", "[0", end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertEquals(5, arrayMessage.children().size());

        // Verify each entry is a MapRedisMessage with expected fields
        for (var child : arrayMessage.children()) {
            assertInstanceOf(MapRedisMessage.class, child);
        }
    }

    @Test
    void shouldReturnEntriesWithExclusiveStartInclusiveEnd() throws IOException, InterruptedException {
        // Append the first batch of entries
        ByteBuffer[] firstBatch = baseVolumeTestWrapper.getEntries(3);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, firstBatch);
            tr.commit().join();
        }

        Thread.sleep(250);

        // Record the sequence number boundary
        long boundarySequenceNumber = hlc.next(System.currentTimeMillis());

        Thread.sleep(250);

        // Append the second batch of entries
        ByteBuffer[] secondBatch = baseVolumeTestWrapper.getEntries(2);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, secondBatch);
            tr.commit().join();
        }

        // Query with exclusive start, inclusive end: (boundary end]
        // Should only return the second batch (2 entries)
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = String.format("(%d", boundarySequenceNumber);
        String end = String.format("%s]", hlc.next(System.currentTimeMillis()));
        cmd.changelogRange(volumeConfig.name(), "*", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertEquals(2, arrayMessage.children().size());
    }

    @Test
    void shouldReturnEntriesWithInclusiveStartExclusiveEnd() throws IOException, InterruptedException {
        // Append the first batch of entries
        ByteBuffer[] firstBatch = baseVolumeTestWrapper.getEntries(3);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, firstBatch);
            tr.commit().join();
        }

        Thread.sleep(250);

        // Record the sequence number boundary
        long boundarySequenceNumber = hlc.next(System.currentTimeMillis());

        Thread.sleep(250);

        // Append the second batch of entries
        ByteBuffer[] secondBatch = baseVolumeTestWrapper.getEntries(2);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, secondBatch);
            tr.commit().join();
        }

        // Query with inclusive start, exclusive end: [0 boundary)
        // Should only return the first batch (3 entries)
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0";
        String end = String.format("%d)", boundarySequenceNumber);
        cmd.changelogRange(volumeConfig.name(), "*", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertEquals(3, arrayMessage.children().size());
    }

    @Test
    void shouldReturnEntriesWithExclusiveRange() throws IOException, InterruptedException {
        // Append the first batch of entries
        ByteBuffer[] firstBatch = baseVolumeTestWrapper.getEntries(2);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, firstBatch);
            tr.commit().join();
        }

        Thread.sleep(250);

        // Record the start boundary
        long startBoundary = hlc.next(System.currentTimeMillis());

        Thread.sleep(250);

        // Append the middle batch of entries
        ByteBuffer[] middleBatch = baseVolumeTestWrapper.getEntries(3);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, middleBatch);
            tr.commit().join();
        }

        Thread.sleep(250);

        // Record the end boundary
        long endBoundary = hlc.next(System.currentTimeMillis());

        Thread.sleep(250);

        // Append the last batch of entries
        ByteBuffer[] lastBatch = baseVolumeTestWrapper.getEntries(2);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, lastBatch);
            tr.commit().join();
        }

        // Query with exclusive start and exclusive end: (startBoundary endBoundary)
        // Should only return the middle batch (3 entries)
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = String.format("(%d", startBoundary);
        String end = String.format("%d)", endBoundary);
        cmd.changelogRange(volumeConfig.name(), "*", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertEquals(3, arrayMessage.children().size());
    }

    @Test
    void shouldFilterByLifecycleOperationKind() throws IOException {
        // Append entries to create APPEND changelog entries (LIFECYCLE)
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(3);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Query with LIFECYCLE filter - should return all 3 APPEND entries
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0";
        String end = String.format("%s]", hlc.next(System.currentTimeMillis()));
        cmd.changelogRange(volumeConfig.name(), "LIFECYCLE", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertEquals(3, arrayMessage.children().size());
    }

    @Test
    void shouldFilterByFinalizationOperationKind() throws IOException {
        // Append entries to create APPEND changelog entries (LIFECYCLE)
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(3);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Query with FINALIZATION filter - should return 0 entries (no DELETE operations)
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0";
        String end = String.format("%s]", hlc.next(System.currentTimeMillis()));
        cmd.changelogRange(volumeConfig.name(), "FINALIZATION", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertEquals(0, arrayMessage.children().size());
    }

    @Test
    void shouldRespectLimitParameter() throws IOException {
        // Append entries to create changelog entries
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Query with LIMIT 2
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0";
        String end = String.format("%s]", hlc.next(System.currentTimeMillis()));
        cmd.changelogRange(volumeConfig.name(), "*", start, end,
                new com.kronotop.cluster.client.protocol.ChangeLogRangeArgs().limit(2)).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertEquals(2, arrayMessage.children().size());
    }

    @Test
    void shouldMapAppendEntryCorrectly() throws IOException {
        // Append a single entry to create an APPEND changelog entry
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(1);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0";
        String end = String.format("%s]", hlc.next(System.currentTimeMillis()));
        cmd.changelogRange(volumeConfig.name(), "*", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertEquals(1, arrayMessage.children().size());

        // Verify APPEND entry structure
        assertInstanceOf(MapRedisMessage.class, arrayMessage.children().getFirst());
        MapRedisMessage entryMap = (MapRedisMessage) arrayMessage.children().getFirst();

        // Extract keys as strings
        Set<String> keys = new HashSet<>();
        String kindValue = null;
        for (var entry : entryMap.children().entrySet()) {
            String key = ((SimpleStringRedisMessage) entry.getKey()).content();
            keys.add(key);
            if (key.equals("kind")) {
                kindValue = ((SimpleStringRedisMessage) entry.getValue()).content();
            }
        }

        // Should have: versionstamp, kind, prefix, after (no before for APPEND)
        assertTrue(keys.contains("versionstamp"));
        assertTrue(keys.contains("kind"));
        assertTrue(keys.contains("prefix"));
        assertTrue(keys.contains("after"));
        assertFalse(keys.contains("before"));

        // Verify kind is APPEND
        assertEquals("APPEND", kindValue);
    }

    @Test
    void shouldMapDeleteEntryCorrectly() throws IOException {
        // Append an entry first
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(1);
        Versionstamp[] keys;
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        // Delete the entry to create a DELETE changelog entry
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.delete(session, keys);
            tr.commit().join();
        }

        // Query with FINALIZATION filter to get only DELETE entries
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0";
        String end = String.format("%s]", hlc.next(System.currentTimeMillis()));
        cmd.changelogRange(volumeConfig.name(), "FINALIZATION", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertEquals(1, arrayMessage.children().size());

        // Verify DELETE entry structure
        assertInstanceOf(MapRedisMessage.class, arrayMessage.children().getFirst());
        MapRedisMessage entryMap = (MapRedisMessage) arrayMessage.children().getFirst();

        // Extract keys as strings
        Set<String> mapKeys = new HashSet<>();
        String kindValue = null;
        for (var entry : entryMap.children().entrySet()) {
            String key = ((SimpleStringRedisMessage) entry.getKey()).content();
            mapKeys.add(key);
            if (key.equals("kind")) {
                kindValue = ((SimpleStringRedisMessage) entry.getValue()).content();
            }
        }

        // Should have: versionstamp, kind, prefix, before (no after for DELETE)
        assertTrue(mapKeys.contains("versionstamp"));
        assertTrue(mapKeys.contains("kind"));
        assertTrue(mapKeys.contains("prefix"));
        assertTrue(mapKeys.contains("before"));
        assertFalse(mapKeys.contains("after"));

        // Verify kind is DELETE
        assertEquals("DELETE", kindValue);
    }

    @Test
    void shouldMapUpdateEntryCorrectly() throws IOException, KeyNotFoundException {
        // Append an entry first
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(1);
        Versionstamp[] keys;
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            keys = appendResult.getVersionstampedKeys();
        }

        // Update the entry to create an UPDATE changelog entry
        ByteBuffer[] updatedEntries = baseVolumeTestWrapper.getEntries(1);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.update(session, new KeyEntry(keys[0], updatedEntries[0]));
            tr.commit().join();
        }

        // Query all entries and find the UPDATE entry
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0";
        String end = String.format("%s]", hlc.next(System.currentTimeMillis()));
        cmd.changelogRange(volumeConfig.name(), "*", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        // Should have 2 entries: APPEND and UPDATE
        assertEquals(2, arrayMessage.children().size());

        // Find the UPDATE entry
        MapRedisMessage updateEntry = null;
        for (var child : arrayMessage.children()) {
            MapRedisMessage entryMap = (MapRedisMessage) child;
            for (var entry : entryMap.children().entrySet()) {
                String key = ((SimpleStringRedisMessage) entry.getKey()).content();
                if (key.equals("kind")) {
                    String kind = ((SimpleStringRedisMessage) entry.getValue()).content();
                    if (kind.equals("UPDATE")) {
                        updateEntry = entryMap;
                        break;
                    }
                }
            }
        }

        assertNotNull(updateEntry);

        // Extract keys as strings
        Set<String> mapKeys = new HashSet<>();
        for (var entry : updateEntry.children().entrySet()) {
            String key = ((SimpleStringRedisMessage) entry.getKey()).content();
            mapKeys.add(key);
        }

        // Should have: versionstamp, kind, prefix, before AND after for UPDATE
        assertTrue(mapKeys.contains("versionstamp"));
        assertTrue(mapKeys.contains("kind"));
        assertTrue(mapKeys.contains("prefix"));
        assertTrue(mapKeys.contains("before"));
        assertTrue(mapKeys.contains("after"));
    }

    @Test
    void shouldRejectInvalidStartFormat() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        // Invalid start format: missing bracket
        String start = "100";
        String end = "200]";
        cmd.changelogRange(volumeConfig.name(), "*", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Invalid start format"));
    }

    @Test
    void shouldRejectInvalidEndFormat() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        // Invalid end format: missing bracket
        String start = "[100";
        String end = "200";
        cmd.changelogRange(volumeConfig.name(), "*", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Invalid end format"));
    }

    @Test
    void shouldRejectStartGreaterThanEnd() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        // Start value greater than end value
        String start = "[200";
        String end = "100]";
        cmd.changelogRange(volumeConfig.name(), "*", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("cannot be greater than"));
    }

    @Test
    void shouldRejectUnknownParentOperationKind() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0";
        String end = "100]";
        cmd.changelogRange(volumeConfig.name(), "UNKNOWN", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Unknown"));
    }

    @Test
    void shouldRejectVolumeNotFound() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String start = "[0";
        String end = "100]";
        cmd.changelogRange("nonexistent-volume", "*", start, end, null).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("invalid volume name"));
    }

    @Test
    void shouldReturnEntriesInReverseOrder() throws IOException, InterruptedException {
        // Append entries in separate transactions to get distinct sequence numbers
        for (int i = 0; i < 3; i++) {
            ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(1);
            try (Transaction tr = database.createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                volume.append(session, entries);
                tr.commit().join();
            }
            Thread.sleep(50);
        }

        String end = String.format("%s]", hlc.next(System.currentTimeMillis()));

        // Query without REVERSE (forward order)
        InternalCommandBuilder<String, String> cmdForward = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf bufForward = Unpooled.buffer();
        cmdForward.changelogRange(volumeConfig.name(), "*", "[0", end, null).encode(bufForward);
        Object msgForward = runCommand(channel, bufForward);

        assertInstanceOf(ArrayRedisMessage.class, msgForward);
        ArrayRedisMessage forwardResult = (ArrayRedisMessage) msgForward;

        // Query with REVERSE
        InternalCommandBuilder<String, String> cmdReverse = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf bufReverse = Unpooled.buffer();
        cmdReverse.changelogRange(volumeConfig.name(), "*", "[0", end,
                new ChangeLogRangeArgs().reverse()).encode(bufReverse);
        Object msgReverse = runCommand(channel, bufReverse);

        assertInstanceOf(ArrayRedisMessage.class, msgReverse);
        ArrayRedisMessage reverseResult = (ArrayRedisMessage) msgReverse;

        assertEquals(3, forwardResult.children().size());
        assertEquals(3, reverseResult.children().size());

        // Verify reverse order by comparing first and last entries
        MapRedisMessage firstForward = (MapRedisMessage) forwardResult.children().getFirst();
        MapRedisMessage lastForward = (MapRedisMessage) forwardResult.children().getLast();
        MapRedisMessage firstReverse = (MapRedisMessage) reverseResult.children().getFirst();
        MapRedisMessage lastReverse = (MapRedisMessage) reverseResult.children().getLast();

        // First entry in forward should equal last entry in reverse
        // Last entry in forward should equal first entry in reverse
        assertArrayEquals(getVersionstamp(firstForward), getVersionstamp(lastReverse));
        assertArrayEquals(getVersionstamp(lastForward), getVersionstamp(firstReverse));
    }

    private byte[] getVersionstamp(MapRedisMessage entry) {
        for (var e : entry.children().entrySet()) {
            String key = ((SimpleStringRedisMessage) e.getKey()).content();
            if (key.equals("versionstamp")) {
                SimpleStringRedisMessage m = (SimpleStringRedisMessage) e.getValue();
                return VersionstampUtil.base32HexDecode(m.content()).getBytes();
            }
        }
        throw new IllegalStateException("versionstamp not found");
    }

    private long getSequenceNumber(MapRedisMessage entry) {
        for (var e : entry.children().entrySet()) {
            String key = ((SimpleStringRedisMessage) e.getKey()).content();
            if (key.equals("after")) {
                MapRedisMessage afterMap = (MapRedisMessage) e.getValue();
                for (var afterEntry : afterMap.children().entrySet()) {
                    String afterKey = ((SimpleStringRedisMessage) afterEntry.getKey()).content();
                    if (afterKey.equals("sequence_number")) {
                        return ((IntegerRedisMessage) afterEntry.getValue()).value();
                    }
                }
            }
        }
        throw new IllegalStateException("sequence_number not found in after");
    }

    @Test
    void shouldReturnSubsetUsingActualSequenceNumbers() throws IOException {
        // Append 5 entries in a single transaction
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // First, scan all entries to discover actual sequence numbers
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        String end = String.format("%d]", hlc.next(System.currentTimeMillis()));
        cmd.changelogRange(volumeConfig.name(), "*", "[0", end, null).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage allEntries = (ArrayRedisMessage) msg;
        assertEquals(5, allEntries.children().size());

        // Extract sequence numbers from all entries
        long[] sequenceNumbers = new long[5];
        for (int i = 0; i < 5; i++) {
            MapRedisMessage entry = (MapRedisMessage) allEntries.children().get(i);
            sequenceNumbers[i] = getSequenceNumber(entry);
        }

        // Query using actual sequence numbers: (seq[1] seq[3]]
        // This should return entries at index 2 and 3 (exclusive start, inclusive end)
        InternalCommandBuilder<String, String> subsetCmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf subsetBuf = Unpooled.buffer();
        String subsetStart = String.format("(%d", sequenceNumbers[1]);
        String subsetEnd = String.format("%d]", sequenceNumbers[3]);
        subsetCmd.changelogRange(volumeConfig.name(), "*", subsetStart, subsetEnd, null).encode(subsetBuf);

        Object subsetMsg = runCommand(channel, subsetBuf);
        assertInstanceOf(ArrayRedisMessage.class, subsetMsg);
        ArrayRedisMessage subsetEntries = (ArrayRedisMessage) subsetMsg;

        // Should return 2 entries (index 2 and 3)
        assertEquals(2, subsetEntries.children().size());

        // Verify the returned entries have the expected sequence numbers
        long firstSeq = getSequenceNumber((MapRedisMessage) subsetEntries.children().get(0));
        long secondSeq = getSequenceNumber((MapRedisMessage) subsetEntries.children().get(1));

        assertEquals(sequenceNumbers[2], firstSeq);
        assertEquals(sequenceNumbers[3], secondSeq);
    }
}