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

package com.kronotop.volume;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TestUtil;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.volume.changelog.ChangeLogCoordinate;
import com.kronotop.volume.changelog.ChangeLogEntry;
import com.kronotop.volume.changelog.ChangeLogIterable;
import com.kronotop.volume.handlers.PackedEntry;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.kronotop.volume.SegmentStatsSubspaces.CARDINALITY;
import static com.kronotop.volume.Subspaces.ENTRY_SUBSPACE;
import static com.kronotop.volume.Subspaces.SEGMENT_STATS_SUBSPACE;
import static org.junit.jupiter.api.Assertions.*;

class VolumeTest extends BaseVolumeIntegrationTest {

    @Test
    void shouldAppendEntriesToVolume() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }
        assertEquals(2, result.getVersionstampedKeys().length);
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenAppendingWithoutEntries() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.append(session));
        }
    }

    @Test
    void shouldGetEntriesByVersionstamp() throws IOException {
        ByteBuffer[] entries = getEntries(3);

        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        List<ByteBuffer> retrievedEntries = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (Versionstamp versionstamp : versionstampedKeys) {
                ByteBuffer buffer = volume.get(session, versionstamp);
                retrievedEntries.add(buffer);
            }
        }
        for (int i = 0; i < retrievedEntries.size(); i++) {
            assertEquals(entries[i].rewind(), retrievedEntries.get(i));
        }
    }

    @Test
    void shouldDeleteEntriesAndReturnNullOnGet() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            deleteResult = volume.delete(session, versionstampedKeys);
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (Versionstamp versionstamp : versionstampedKeys) {
                assertNull(volume.get(session, versionstamp));
            }
        }

        VolumeSession session = new VolumeSession(prefix);
        // EntryMetadata cache
        for (Versionstamp versionstamp : versionstampedKeys) {
            assertNull(volume.get(session, versionstamp));
        }
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenDeletingWithoutKeys() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.delete(session));
        }
    }

    @Test
    void shouldUpdateEntriesWithNewValues() throws IOException, KeyNotFoundException {
        Versionstamp[] versionstampedKeys;

        {
            ByteBuffer[] entries = {
                    ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                    ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
            };
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                result = volume.append(session, entries);
                tr.commit().join();
            }
            versionstampedKeys = result.getVersionstampedKeys();
        }

        {
            KeyEntry[] entries = new KeyEntry[2];
            entries[0] = new KeyEntry(versionstampedKeys[0], ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip());
            entries[1] = new KeyEntry(versionstampedKeys[1], ByteBuffer.allocate(6).put("BARFOO".getBytes()).flip());
            UpdateResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                result = volume.update(session, entries);
                tr.commit().join();
            }
            result.complete();

            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                for (Versionstamp versionstamp : versionstampedKeys) {
                    ByteBuffer buffer = volume.get(session, versionstamp);
                    retrievedEntries.add(buffer);
                }
            }
            for (int i = 0; i < retrievedEntries.size(); i++) {
                assertEquals(entries[i].entry().rewind(), retrievedEntries.get(i));
            }
        }
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenUpdatingWithoutEntries() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.update(session));
        }
    }

    @Test
    void shouldThrowEntrySizeExceedsLimitExceptionWhenUpdatingWithOversizedEntry() throws IOException {
        Versionstamp versionstampedKey;

        // First, append a small entry to get a key
        ByteBuffer smallEntry = ByteBuffer.allocate(10).put("small".getBytes()).flip();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            AppendResult result = volume.append(session, smallEntry);
            tr.commit().join();
            versionstampedKey = result.getVersionstampedKeys()[0];
        }

        // Try to update with an oversized entry
        ByteBuffer oversizedEntry = randomBytes(Volume.ENTRY_SIZE_LIMIT + 1);
        KeyEntry oversizedPair = new KeyEntry(versionstampedKey, oversizedEntry);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(EntrySizeExceedsLimitException.class, () -> volume.update(session, oversizedPair));
        }
    }

    @Test
    void shouldFlushWithoutError() {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertDoesNotThrow(() -> volume.append(session, entries));
            tr.commit().join();
        }
        assertDoesNotThrow(() -> volume.flush());
    }

    @Test
    void shouldCloseWithoutError() {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertDoesNotThrow(() -> volume.append(session, entries));
            tr.commit().join();
        }
        assertDoesNotThrow(() -> volume.close());
    }

    @Test
    void shouldReopenVolumeAndRetrieveEntriesAfterClose() throws IOException {
        Versionstamp[] versionstampedKeys;
        ByteBuffer[] entries = getEntries(2);
        {
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                result = volume.append(session, entries);
                tr.commit().join();
            }
            versionstampedKeys = result.getVersionstampedKeys();
        }

        volume.close();

        {
            Volume reopenedVolume = service.newVolume(volume.getConfig());
            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                for (Versionstamp versionstamp : versionstampedKeys) {
                    ByteBuffer buffer = reopenedVolume.get(session, versionstamp);
                    retrievedEntries.add(buffer);
                }
            }
            for (int i = 0; i < retrievedEntries.size(); i++) {
                assertEquals(entries[i].rewind(), retrievedEntries.get(i));
            }
        }
    }

    @Test
    void shouldDetermineSegmentWritePositionFromPersistedMetadata() throws IOException {
        byte[] data = new byte[]{0x01, 0x02, 0x03};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            volume.append(session, ByteBuffer.wrap(data));
            tr.commit().join();
        }
        List<SegmentAnalysis> analysis = volume.analyze();
        assertEquals(1, analysis.size());

        volume.close();

        SegmentAnalysis segmentAnalysis = analysis.getFirst();
        long position = SegmentSubspaceUtil.findNextPosition(context, volume.getConfig().subspace(), segmentAnalysis.segmentId());
        assertEquals(data.length, position);
    }

    @Test
    void shouldWriteNewEntriesAfterReopeningVolume() throws IOException {
        List<Versionstamp> versionstampedKeys;

        ByteBuffer[] firstEntries = getEntries(2);
        {
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                result = volume.append(session, firstEntries);
                tr.commit().join();
            }
            versionstampedKeys = new ArrayList<>(Arrays.stream(result.getVersionstampedKeys()).toList());
        }

        // Flushes all the underlying files.
        volume.close();

        {
            Volume reopenedVolume = service.newVolume(volume.getConfig());

            ByteBuffer[] secondEntries = getEntries(2);
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                result = reopenedVolume.append(session, secondEntries);
                tr.commit().join();
            }
            versionstampedKeys.addAll(Arrays.stream(result.getVersionstampedKeys()).toList());

            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                for (Versionstamp versionstamp : versionstampedKeys) {
                    ByteBuffer buffer = reopenedVolume.get(session, versionstamp);
                    retrievedEntries.add(buffer);
                }
            }

            for (int i = 0; i < firstEntries.length; i++) {
                assertEquals(firstEntries[i].rewind(), retrievedEntries.get(i));
            }

            for (int i = 0; i < secondEntries.length; i++) {
                assertEquals(secondEntries[i].rewind(), retrievedEntries.get(i));
            }
        }
    }

    @Test
    void shouldCreateNewSegmentsWhenCapacityReached() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 1; i <= numIterations; i++) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        assertEquals(2, volume.analyze().size());
    }

    @Test
    void shouldHandleConcurrentAppendsAndRetrieveAllKeys() throws IOException, InterruptedException {
        ConcurrentHashMap<Versionstamp, ByteBuffer> pairs = new ConcurrentHashMap<>();
        int numberOfThreads = Runtime.getRuntime().availableProcessors() * 2;
        int entriesPerThread = 2;
        CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);

        class AppendRunner implements Runnable {
            private final ByteBuffer[] entries;

            AppendRunner(ByteBuffer[] entries) {
                this.entries = entries;
            }

            @Override
            public void run() {
                AppendResult result;
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                    result = volume.append(session, entries);
                    tr.commit().join();
                    Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
                    for (int i = 0; i < versionstampedKeys.length; i++) {
                        Versionstamp versionstampedKey = versionstampedKeys[i];
                        pairs.put(versionstampedKey, entries[i]);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            }
        }

        try (ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads)) {
            for (int i = 0; i < numberOfThreads; i++) {
                AppendRunner appendRunner = new AppendRunner(getEntries(entriesPerThread));
                executor.execute(appendRunner);
            }
            countDownLatch.await();
        }

        assertEquals(numberOfThreads * entriesPerThread, pairs.size());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (Map.Entry<Versionstamp, ByteBuffer> entry : pairs.entrySet()) {
                ByteBuffer buffer = volume.get(session, entry.getKey());
                assertEquals(entry.getValue().rewind(), buffer);
            }
        }
    }

    @Test
    void shouldAnalyzeSegmentsCorrectly() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> segmentAnalysis = volume.analyze(tr);
            assertEquals(2, segmentAnalysis.size());

            int cardinality = 0;
            for (SegmentAnalysis analysis : segmentAnalysis) {
                assertEquals(0.0, analysis.garbagePercentage());
                assertEquals(segmentSize, analysis.size());
                assertEquals(analysis.freeBytes(), analysis.size() - analysis.usedBytes());
                assertTrue(analysis.cardinality() > 0);
                cardinality += analysis.cardinality();
            }
            assertEquals(numIterations, cardinality);
        }
    }

    @Test
    void shouldAnalyzeEmptyWritableSegment() {
        // Behavior: A fresh volume with no appends has one writable segment
        // with zero cardinality, zero usedBytes, and full freeBytes.
        List<SegmentAnalysis> analysis = volume.analyze();
        assertEquals(1, analysis.size());

        SegmentAnalysis segmentAnalysis = analysis.getFirst();
        assertEquals(0, segmentAnalysis.cardinality());
        assertEquals(0, segmentAnalysis.usedBytes());
        assertEquals(segmentAnalysis.size(), segmentAnalysis.freeBytes());
        assertEquals(0.0, segmentAnalysis.garbagePercentage());
    }

    @Test
    void shouldAnalyzeSingleWritableSegment() throws IOException {
        // Behavior: After a single small append, analyze returns exactly one segment
        // with correct cardinality, usedBytes, freeBytes, and zero garbage.
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            volume.append(session, ByteBuffer.wrap(data));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> analysis = volume.analyze(tr);
            assertEquals(1, analysis.size());

            SegmentAnalysis segmentAnalysis = analysis.getFirst();
            assertEquals(1, segmentAnalysis.cardinality());
            assertEquals(data.length, segmentAnalysis.usedBytes());
            assertEquals(segmentAnalysis.size() - data.length, segmentAnalysis.freeBytes());
            assertEquals(0.0, segmentAnalysis.garbagePercentage());
        }
    }

    @Test
    void shouldReportCorrectUsedBytesAcrossMultipleSegments() throws IOException {
        // Behavior: Total usedBytes across all segments equals the sum of bytes actually written.
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> segmentAnalysis = volume.analyze(tr);
            assertEquals(2, segmentAnalysis.size());

            long totalUsedBytes = segmentAnalysis.stream().mapToLong(SegmentAnalysis::usedBytes).sum();
            assertEquals(numIterations * bufferSize, totalUsedBytes);
        }
    }

    @Test
    void shouldCalculateGarbagePercentageAfterDeletingEntries() throws IOException {
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, getEntries(10));
            tr.commit().join();
        }

        DeleteResult deleteResult;
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();
        Versionstamp[] keys = Arrays.copyOfRange(versionstampedKeys, 3, 7);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            deleteResult = volume.delete(session, keys);
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> analysis = volume.analyze(tr);
            SegmentAnalysis segmentAnalysis = analysis.getFirst();
            assertTrue(segmentAnalysis.garbagePercentage() > 0);
            long garbageBytes = segmentAnalysis.size() - segmentAnalysis.freeBytes() - segmentAnalysis.usedBytes();
            assertEquals(40, garbageBytes); // We deleted 4 items and the test entry size is 10.
        }
    }

    @Test
    void shouldThrowEntrySizeExceedsLimitExceptionWhenEntryTooLarge() {
        ByteBuffer oversizedEntry = randomBytes(Volume.ENTRY_SIZE_LIMIT + 1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(EntrySizeExceedsLimitException.class, () -> volume.append(session, oversizedEntry));
        }
    }

    @Test
    void shouldThrowTooManyEntriesExceptionWhenExceedingMaxEntries() {
        ByteBuffer[] entries = getEntries(UserVersion.MAX_VALUE + 1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(TooManyEntriesException.class, () -> volume.append(session, entries));
        }
    }

    @Test
    void shouldThrowTooManyEntriesExceptionWhenExceedingSessionLimit() throws IOException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            int batchSize = UserVersion.MAX_VALUE / 5;
            for (int i = 0; i < 5; i++) {
                ByteBuffer[] entries = getEntries(batchSize);
                volume.append(session, entries);
            }
            ByteBuffer[] entries = getEntries(2);
            assertThrows(TooManyEntriesException.class, () -> volume.append(session, entries));
        }
    }

    @Test
    void shouldUpdateSegmentCardinalityWhenUpdatingEntries() throws IOException, KeyNotFoundException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        AppendResult result;
        ByteBuffer[] entries = new ByteBuffer[(int) numIterations];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 1; i <= numIterations; i++) {
                entries[i - 1] = randomBytes((int) bufferSize);
            }
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        KeyEntry[] pairs = new KeyEntry[versionstampedKeys.length];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            int index = 0;
            for (Versionstamp versionstampedKey : versionstampedKeys) {
                pairs[index] = new KeyEntry(versionstampedKey, randomBytes((int) bufferSize));
                index++;
            }
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            UpdateResult updateResult = volume.update(session, pairs);
            tr.commit().join();
            updateResult.complete();
        }

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        assertEquals(4, segmentAnalysis.size());

        long staleCount = segmentAnalysis.stream().filter(a -> a.cardinality() == 0).count();
        long liveCount = segmentAnalysis.stream().filter(a -> a.cardinality() > 0).count();
        assertEquals(2, staleCount);
        assertEquals(2, liveCount);

        for (SegmentAnalysis analysis : segmentAnalysis) {
            if (analysis.cardinality() > 0) {
                assertEquals(10, analysis.cardinality());
            }
        }
    }

    @Test
    void shouldGetRangeForFullScan() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        Versionstamp[] retrievedKeys = new Versionstamp[10];
        ByteBuffer[] retrievedEntries = new ByteBuffer[10];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            int index = 0;
            Iterable<VolumeEntry> iterable = volume.getRange(session);
            for (VolumeEntry keyEntry : iterable) {
                retrievedKeys[index] = keyEntry.key();
                retrievedEntries[index] = keyEntry.entry();
                index++;
            }
        }
        assertArrayEquals(versionstampedKeys, retrievedKeys);
        for (int i = 0; i < entries.length; i++) {
            assertEquals(entries[i].flip(), retrievedEntries[i]);
        }
    }

    @Test
    void shouldGetRangeWithLimit() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        // Retrieves the first 5 keys.
        int limit = 5;
        Versionstamp[] retrievedKeys = new Versionstamp[limit];
        ByteBuffer[] retrievedEntries = new ByteBuffer[limit];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            int index = 0;
            Iterable<VolumeEntry> iterable = volume.getRange(session, limit);
            for (VolumeEntry keyEntry : iterable) {
                retrievedKeys[index] = keyEntry.key();
                retrievedEntries[index] = keyEntry.entry();
                index++;
            }
        }

        assertArrayEquals(Arrays.copyOfRange(versionstampedKeys, 0, limit), retrievedKeys);
        for (int i = 0; i < limit; i++) {
            assertEquals(entries[i].flip(), retrievedEntries[i]);
        }
    }

    @Test
    void shouldGetRangeInReverseOrder() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        // Retrieves the first 5 keys.
        Versionstamp[] retrievedKeys = new Versionstamp[10];
        ByteBuffer[] retrievedEntries = new ByteBuffer[10];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            int index = 0;
            Iterable<VolumeEntry> iterable = volume.getRange(session, true);
            for (VolumeEntry keyEntry : iterable) {
                retrievedKeys[index] = keyEntry.key();
                retrievedEntries[index] = keyEntry.entry();
                index++;
            }
        }

        // Reverse keys and entries first.
        Collections.reverse(Arrays.asList(versionstampedKeys));
        Collections.reverse(Arrays.asList(entries));

        assertArrayEquals(versionstampedKeys, retrievedKeys);
        for (int i = 0; i < 10; i++) {
            assertEquals(entries[i].flip(), retrievedEntries[i]);
        }
    }

    @Test
    void shouldGetRangeWithCustomSelectors() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] expectedKeys = Arrays.copyOfRange(result.getVersionstampedKeys(), 3, 7);
        ByteBuffer[] expectedEntries = new ByteBuffer[expectedKeys.length];

        VolumeSession sessionWithoutTransaction = new VolumeSession(stashVolumeSyncerPrefix);
        for (int i = 0; i < expectedKeys.length; i++) {
            Versionstamp key = expectedKeys[i];
            ByteBuffer entry = volume.get(sessionWithoutTransaction, key);
            expectedEntries[i] = entry;
        }

        Versionstamp[] retrievedKeys = new Versionstamp[expectedKeys.length];
        ByteBuffer[] retrievedEntries = new ByteBuffer[expectedKeys.length];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(expectedKeys[0]);
            VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterThan(expectedKeys[expectedKeys.length - 1]);

            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = volume.getRange(session, begin, end);
            int index = 0;
            for (VolumeEntry keyEntry : iterable) {
                retrievedKeys[index] = keyEntry.key();
                retrievedEntries[index] = keyEntry.entry();
                index++;
            }
        }

        assertArrayEquals(expectedKeys, retrievedKeys);
        for (int i = 0; i < expectedEntries.length; i++) {
            assertEquals(expectedEntries[i], retrievedEntries[i]);
        }
    }

    @Test
    void shouldIsolatePrefixesCorrectly() throws IOException {
        Prefix prefixOne = new Prefix("one");
        Prefix prefixTwo = new Prefix("two");
        String dataOne = "prefix-one-entry";
        String dataTwo = "prefix-two-entry";

        Versionstamp keyOne;
        Versionstamp keyTwo;

        {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefixOne);
                ByteBuffer entry = ByteBuffer.allocate(dataOne.length()).put(dataOne.getBytes()).flip();
                AppendResult result = volume.append(session, entry);
                tr.commit().join();
                keyOne = result.getVersionstampedKeys()[0];
            }
        }

        {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefixTwo);
                ByteBuffer entry = ByteBuffer.allocate(dataTwo.length()).put(dataTwo.getBytes()).flip();
                AppendResult result = volume.append(session, entry);
                tr.commit().join();
                keyTwo = result.getVersionstampedKeys()[0];
            }
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefixOne);
            ByteBuffer entry = volume.get(session, keyTwo);
            assertNull(entry);

            entry = volume.get(session, keyOne);
            assertNotNull(entry);
            assertEquals(ByteBuffer.wrap(dataOne.getBytes()), entry);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefixTwo);
            ByteBuffer entry = volume.get(session, keyOne);
            assertNull(entry);

            entry = volume.get(session, keyTwo);
            assertNotNull(entry);
            assertEquals(ByteBuffer.wrap(dataTwo.getBytes()), entry);
        }

        {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefixOne);
                DeleteResult result = volume.delete(session, keyTwo);
                tr.commit().join();
                result.complete();
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefixTwo);
                ByteBuffer entry = volume.get(session, keyTwo);
                assertNotNull(entry);
                assertEquals(ByteBuffer.wrap(dataTwo.getBytes()), entry);
            }
        }
    }

    @Test
    void shouldGetRangeWithPrefixIsolation() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        {
            int index = 0;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                Iterable<VolumeEntry> iterable = volume.getRange(session);
                for (VolumeEntry ignored : iterable) {
                    index++;
                }
            }
            assertEquals(10, index);
        }

        {
            int index = 0;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, new Prefix("test"));
                Iterable<VolumeEntry> iterable = volume.getRange(session);
                for (VolumeEntry ignored : iterable) {
                    index++;
                }
            }
            assertEquals(0, index);
        }
    }

    @Test
    void shouldClearPrefixAndRemoveAllEntries() throws IOException {
        ByteBuffer[] entries = getEntries(3);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            volume.clearPrefix(session);
            tr.commit().join();
        }

        {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                SegmentAnalysis analysis = volume.analyze(tr).getFirst();
                byte[] begin = SegmentSubspaceUtil.prefixOfVolumePrefix(volume.getConfig().subspace(), analysis.segmentId(), stashVolumeSyncerPrefix);
                Range range = Range.startsWith(begin);
                List<KeyValue> survivedEntries = tr.getRange(range).asList().join();
                assertEquals(0, survivedEntries.size());
            }
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (Versionstamp versionstamp : versionstampedKeys) {
                assertNull(volume.get(session, versionstamp));
            }
        }

        // Prefix cleared
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentAnalysis analysis = volume.analyze(tr).getFirst();
            assertEquals(0, analysis.cardinality());
            assertEquals(0, analysis.usedBytes());
            assertTrue(analysis.size() - analysis.freeBytes() > 0);
        }
    }

    @Test
    void shouldClearOnlySpecificPrefixWhenMultiplePrefixesExist() {
        Prefix prefixOne = new Prefix("prefixOne");
        Prefix prefixTwo = new Prefix("prefixTwo");

        List.of(prefixOne, prefixTwo).forEach((item) -> {
            ByteBuffer[] entries = getEntries(3);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, item);
                AppendResult result;
                try {
                    result = volume.append(session, entries);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                tr.commit().join();
                result.updateEntryMetadataCache();
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefixOne);
            volume.clearPrefix(session);
            tr.commit().join();
        }

        // prefixOne has cleared
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentAnalysis analysis = volume.analyze(tr).getFirst();
            assertEquals(3, analysis.cardinality());
            assertTrue(analysis.usedBytes() > 0);
            assertTrue(analysis.size() - analysis.freeBytes() > 0);
        }
    }

    @Test
    void shouldInsertPackedEntries(@TempDir Path dataDir) throws IOException {
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6};
        ByteBuffer[] entries = {
                ByteBuffer.wrap(first),
                ByteBuffer.wrap(second),
        };
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        VolumeConfig config = new VolumeConfig(
                volume.getConfig().subspace(),
                volume.getConfig().name(),
                dataDir.toString(),
                volume.getConfig().segmentSize()
        );

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        long segmentId = segmentAnalysis.getFirst().segmentId();

        Volume standby = service.newVolume(config);
        PackedEntry[] packedEntries = new PackedEntry[]{
                new PackedEntry(0, first),
                new PackedEntry(3, second)
        };
        standby.insert(segmentId, packedEntries);
        standby.flush();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            Versionstamp[] versionstamps = result.getVersionstampedKeys();

            ByteBuffer firstBuffer = standby.get(session, versionstamps[0]);
            assertEquals(ByteBuffer.wrap(first), firstBuffer);

            ByteBuffer secondBuffer = standby.get(session, versionstamps[1]);
            assertEquals(ByteBuffer.wrap(second), secondBuffer);
        }
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenInsertingWithoutEntries() {
        assertThrows(IllegalArgumentException.class, () -> volume.insert(123));
    }

    @Test
    void shouldThrowEntrySizeExceedsLimitExceptionWhenInsertingOversizedEntry() throws IOException {
        byte[] oversizedData = new byte[Volume.ENTRY_SIZE_LIMIT + 1];
        List<SegmentAnalysis> segmentAnalysis = volume.analyze();

        // If no segments exist yet, create one by appending a small entry first
        if (segmentAnalysis.isEmpty()) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                volume.append(session, ByteBuffer.wrap(new byte[]{1}));
                tr.commit().join();
            }
            segmentAnalysis = volume.analyze();
        }

        long segmentId = segmentAnalysis.getFirst().segmentId();
        PackedEntry oversizedEntry = new PackedEntry(0, oversizedData);

        assertThrows(EntrySizeExceedsLimitException.class, () -> volume.insert(segmentId, oversizedEntry));
    }

    @Test
    void shouldHaveReadWriteStatusByDefault() {
        assertEquals(VolumeStatus.READWRITE, volume.getStatus());
    }

    @Test
    void shouldSetAndPersistVolumeStatus() {
        volume.setStatus(VolumeStatus.READONLY);
        assertEquals(VolumeStatus.READONLY, volume.getStatus());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeStatus status = VolumeMetadataUtil.readVolumeStatus(tr, volume.getSubspace());
            assertEquals(VolumeStatus.READONLY, status);
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionWhenAppendingToReadOnlyVolume() {
        volume.setStatus(VolumeStatus.READONLY);

        ByteBuffer[] entries = getEntries(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.append(session, entries));
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionWhenDeletingFromReadOnlyVolume() throws IOException {
        ByteBuffer[] entries = getEntries(1);
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        volume.setStatus(VolumeStatus.READONLY);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.delete(session, versionstampedKeys));
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionWhenUpdatingReadOnlyVolume() throws IOException {
        Versionstamp[] versionstampedKeys;

        {
            ByteBuffer[] entries = {
                    ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                    ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
            };
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                result = volume.append(session, entries);
                tr.commit().join();
            }
            versionstampedKeys = result.getVersionstampedKeys();
        }


        volume.setStatus(VolumeStatus.READONLY);
        KeyEntry[] entries = new KeyEntry[2];
        entries[0] = new KeyEntry(versionstampedKeys[0], ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip());
        entries[1] = new KeyEntry(versionstampedKeys[1], ByteBuffer.allocate(6).put("BARFOO".getBytes()).flip());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.update(session, entries));
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionWhenClearingPrefixOnReadOnlyVolume() throws IOException {
        ByteBuffer[] entries = getEntries(3);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        volume.setStatus(VolumeStatus.READONLY);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.clearPrefix(session));
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionWhenInsertingToReadOnlyVolume(@TempDir Path dataDir) throws IOException {
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6};
        ByteBuffer[] entries = {
                ByteBuffer.wrap(first),
                ByteBuffer.wrap(second),
        };
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        VolumeConfig config = new VolumeConfig(
                volume.getConfig().subspace(),
                volume.getConfig().name(),
                dataDir.toString(),
                volume.getConfig().segmentSize()
        );

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        long segmentId = segmentAnalysis.getFirst().segmentId();

        Volume standby = service.newVolume(config);
        standby.setStatus(VolumeStatus.READONLY);
        PackedEntry[] packedEntries = new PackedEntry[]{
                new PackedEntry(0, first),
                new PackedEntry(3, second)
        };
        assertThrows(VolumeReadOnlyException.class, () -> standby.insert(segmentId, packedEntries));
    }

    @Test
    void shouldSetAndGetAttributes() {
        volume.setAttribute(VolumeAttributes.SHARD_ID, 1);
        assertEquals(1, volume.getAttribute(VolumeAttributes.SHARD_ID));
    }

    @Test
    void shouldUnsetAttributeAfterSettingIt() {
        volume.setAttribute(VolumeAttributes.SHARD_ID, 1);
        volume.unsetAttribute(VolumeAttributes.SHARD_ID);
        assertNull(volume.getAttribute(VolumeAttributes.SHARD_ID));
    }

    @Test
    void shouldHandleHighConcurrencyAppendsAndRetrieveAll() throws IOException, InterruptedException {
        int PARALLEL_APPENDS = 100;
        ConcurrentHashMap<String, ByteBuffer> expected = new ConcurrentHashMap<>();

        class Append implements Runnable {
            final CountDownLatch latch;

            Append(CountDownLatch latch) {
                this.latch = latch;
            }

            @Override
            public void run() {
                ByteBuffer[] entries = new ByteBuffer[10];
                List<String> values = new ArrayList<>();

                for (int i = 0; i < entries.length; i++) {
                    String value = UUID.randomUUID().toString();
                    values.add(value);
                    entries[i] = ByteBuffer.wrap(value.getBytes());
                }

                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                    AppendResult result = volume.append(session, entries);
                    tr.commit().join();
                    Versionstamp[] keys = result.getVersionstampedKeys();
                    for (int i = 0; i < keys.length; i++) {
                        Versionstamp key = keys[i];
                        expected.put(VersionstampUtil.base32HexEncode(key), ByteBuffer.wrap(values.get(i).getBytes()));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    latch.countDown();
                }
            }
        }

        CountDownLatch latch = new CountDownLatch(PARALLEL_APPENDS);
        for (int i = 0; i < PARALLEL_APPENDS; i++) {
            Thread.ofVirtual().start(new Append(latch));
        }
        latch.await();

        ConcurrentHashMap<String, ByteBuffer> result = new ConcurrentHashMap<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (String key : expected.keySet()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                ByteBuffer buffer = volume.get(session, VersionstampUtil.base32HexDecode(key));
                assertNotNull(buffer);
                result.put(key, buffer);
            }
        }
        assertEquals(expected, result);
    }

    @Test
    void shouldSetNonZeroIdDuringVolumeInitialization() {
        // Test that volume initialization correctly sets a non-zero ID in VolumeMetadata
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long volumeId = VolumeMetadataUtil.readVolumeId(tr, new VolumeSubspace(subspace));

            // The initialize() method should have set a non-zero ID during volume construction
            assertNotEquals(0, volumeId, "Volume ID should not be 0 after initialization");

            // Verify the ID is actually a valid integer (not zero)
            assertTrue(volumeId != 0, "Volume ID must be set to a non-zero value");
        }
    }

    @Test
    void shouldPreventUnnecessarySegmentCreationWhenReopening() throws IOException {
        // Test that openSegments correctly opens existing segments and prevents creating new segments on reopen

        // Create initial volume and append data to force segment creation
        ByteBuffer[] initialEntries = getEntries(5);
        List<Long> segmentsAfterInitialAppend;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            volume.append(session, initialEntries);
            tr.commit().join();
        }

        // Check how many segments exist after the initial append.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, new VolumeSubspace(volume.getConfig().subspace()));
            segmentsAfterInitialAppend = new ArrayList<>(segmentIds);
        }

        assertFalse(segmentsAfterInitialAppend.isEmpty(), "Initial append should create at least one segment");

        // Close the volume
        volume.close();

        // Reopen the volume (this calls openSegments in constructor)
        Volume reopenedVolume = service.newVolume(volume.getConfig());

        // Append new data to the reopened volume
        ByteBuffer[] newEntries = getEntries(3);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            reopenedVolume.append(session, newEntries);
            tr.commit().join();
        }

        List<Long> segmentsAfterReopen;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, new VolumeSubspace(reopenedVolume.getConfig().subspace()));
            segmentsAfterReopen = new ArrayList<>(segmentIds);
        }

        // Verify that openSegments prevented unnecessary segment creation
        // The segment count should be the same unless the existing segment was full
        // In most cases, no new segments should be created for small appends
        assertEquals(segmentsAfterInitialAppend, segmentsAfterReopen);

        // Verify that we can still read the original data
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);

            // Get all data using range scan to verify both old and new data are accessible
            List<ByteBuffer> allRetrievedEntries = new ArrayList<>();
            Iterable<VolumeEntry> iterable = reopenedVolume.getRange(session);
            for (VolumeEntry keyEntry : iterable) {
                allRetrievedEntries.add(keyEntry.entry());
            }

            // Should have initial entries + new entries
            assertEquals(initialEntries.length + newEntries.length, allRetrievedEntries.size(),
                    "Should be able to read all entries (original + new) after reopen");
        }

        // Clean up
        reopenedVolume.close();
    }

    @Test
    void shouldGetSegmentRangeAndRetrieveMultipleEntries() throws IOException {
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6};
        byte[] third = new byte[]{7, 8, 9};
        ByteBuffer[] entries = {
                ByteBuffer.wrap(first),
                ByteBuffer.wrap(second),
                ByteBuffer.wrap(third)
        };

        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }
        result.updateEntryMetadataCache();

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        long segmentId = segmentAnalysis.getFirst().segmentId();

        SegmentRange[] segmentRanges = new SegmentRange[]{
                new SegmentRange(0, first.length),
                new SegmentRange(first.length, second.length),
                new SegmentRange(first.length + second.length, third.length)
        };

        ByteBuffer[] retrievedEntries = volume.getSegmentRange(segmentId, segmentRanges);

        assertEquals(3, retrievedEntries.length);
        assertEquals(ByteBuffer.wrap(first), retrievedEntries[0]);
        assertEquals(ByteBuffer.wrap(second), retrievedEntries[1]);
        assertEquals(ByteBuffer.wrap(third), retrievedEntries[2]);
    }

    @Test
    void shouldThrowEntryOutOfBoundExceptionWhenSegmentRangeExceedsSize() throws IOException {
        byte[] data = new byte[]{1, 2, 3};
        ByteBuffer[] entries = {ByteBuffer.wrap(data)};

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        long segmentId = segmentAnalysis.getFirst().segmentId();
        long segmentSize = segmentAnalysis.getFirst().size();

        SegmentRange[] segmentRanges = new SegmentRange[]{
                new SegmentRange(segmentSize - 10, 20)
        };

        assertThrows(EntryOutOfBoundException.class, () -> volume.getSegmentRange(segmentId, segmentRanges));
    }

    @Test
    void shouldRecordEntriesInChangeLog() throws IOException {
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6};
        ByteBuffer[] entries = {ByteBuffer.wrap(first), ByteBuffer.wrap(second)};

        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstamps = result.getVersionstampedKeys();
        assertEquals(2, versionstamps.length);

        List<SegmentAnalysis> analysis = volume.analyze();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(Subspaces.CHANGELOG_SUBSPACE));
            List<KeyValue> changeLogEntries = tr.getRange(range).asList().join();

            assertEquals(2, changeLogEntries.size());

            long previousLogSeq = -1;
            for (int i = 0; i < changeLogEntries.size(); i++) {
                KeyValue kv = changeLogEntries.get(i);
                Tuple keyTuple = subspace.unpack(kv.getKey());
                Tuple valueTuple = Tuple.fromBytes(kv.getValue());

                long logSeq = keyTuple.getLong(1);
                assertTrue(logSeq > previousLogSeq);
                previousLogSeq = logSeq;

                long rawParentOpKind = keyTuple.getLong(2);
                ParentOperationKind parentOpKind = ParentOperationKind.valueOf((byte) rawParentOpKind);
                assertEquals(ParentOperationKind.LIFECYCLE, parentOpKind);

                long rawOpKind = keyTuple.getLong(3);
                OperationKind opKind = OperationKind.valueOf((byte) rawOpKind);
                assertEquals(OperationKind.APPEND, opKind);

                Versionstamp versionstamp = keyTuple.getVersionstamp(4);
                assertEquals(versionstamps[i], versionstamp);

                long segmentId = analysis.getFirst().segmentId();
                assertEquals(segmentId, valueTuple.getLong(0));

                long position = valueTuple.getLong(1);
                assertEquals(i == 0 ? 0 : first.length, position);

                long length = valueTuple.getLong(2);
                assertEquals(i == 0 ? first.length : second.length, length);

                assertEquals(stashVolumeSyncerPrefix.asLong(), valueTuple.getLong(3));
            }
        }
    }

    @Test
    void shouldRecordAppendAndDeleteOperationsInChangeLog() throws IOException {
        byte[] first = new byte[]{1, 2, 3};
        byte[] second = new byte[]{4, 5, 6};
        ByteBuffer[] entries = {ByteBuffer.wrap(first), ByteBuffer.wrap(second)};

        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstamps = appendResult.getVersionstampedKeys();
        assertEquals(2, versionstamps.length);

        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            deleteResult = volume.delete(session, versionstamps);
            tr.commit().join();
        }
        deleteResult.complete();

        List<SegmentAnalysis> analysis = volume.analyze();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(Subspaces.CHANGELOG_SUBSPACE));
            List<KeyValue> changeLogEntries = tr.getRange(range).asList().join();

            assertEquals(4, changeLogEntries.size());

            long previousLogSeq = -1;
            for (int i = 0; i < changeLogEntries.size(); i++) {
                KeyValue kv = changeLogEntries.get(i);
                Tuple keyTuple = subspace.unpack(kv.getKey());
                Tuple valueTuple = Tuple.fromBytes(kv.getValue());

                long logSeq = keyTuple.getLong(1);
                assertTrue(logSeq > previousLogSeq);
                previousLogSeq = logSeq;

                long rawParentOpKind = keyTuple.getLong(2);
                ParentOperationKind parentOpKind = ParentOperationKind.valueOf((byte) rawParentOpKind);

                long rawOpKind = keyTuple.getLong(3);
                OperationKind operationKind = OperationKind.valueOf((byte) rawOpKind);
                Versionstamp versionstamp = keyTuple.getVersionstamp(4);

                if (i < 2) {
                    assertEquals(ParentOperationKind.LIFECYCLE, parentOpKind);
                    assertEquals(OperationKind.APPEND, operationKind);
                    assertEquals(versionstamps[i], versionstamp);

                    long segmentId = analysis.getFirst().segmentId();
                    assertEquals(segmentId, valueTuple.getLong(0));

                    long position = valueTuple.getLong(1);
                    assertEquals(i == 0 ? 0 : first.length, position);

                    long length = valueTuple.getLong(2);
                    assertEquals(i == 0 ? first.length : second.length, length);
                } else {
                    assertEquals(ParentOperationKind.FINALIZATION, parentOpKind);
                    assertEquals(OperationKind.DELETE, operationKind);
                    assertEquals(versionstamps[i - 2], versionstamp);

                    long segmentId = analysis.getFirst().segmentId();
                    assertEquals(segmentId, valueTuple.getLong(0));

                    long position = valueTuple.getLong(1);
                    assertEquals((i - 2) == 0 ? 0 : first.length, position);

                    long length = valueTuple.getLong(2);
                    assertEquals((i - 2) == 0 ? first.length : second.length, length);
                }

                assertEquals(stashVolumeSyncerPrefix.asLong(), valueTuple.getLong(3));
            }
        }
    }

    @Test
    void shouldTriggerWatchers_onAppendOperation() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = tr.get(volume.computeMutationTriggerKey()).join();
            assertNotNull(value);
        }
    }

    @Test
    void shouldTriggerWatchers_onDeleteOperation() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }

        byte[] valueAfterAppend;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            valueAfterAppend = tr.get(volume.computeMutationTriggerKey()).join();
            assertNotNull(valueAfterAppend);
        }

        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            deleteResult = volume.delete(session, appendResult.getVersionstampedKeys());
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] valueAfterDelete = tr.get(volume.computeMutationTriggerKey()).join();
            assertNotNull(valueAfterDelete);
            assertNotEquals(valueAfterAppend, valueAfterDelete);
        }
    }

    @Test
    void shouldTriggerWatchers_onUpdateOperation() throws IOException, KeyNotFoundException {
        ByteBuffer[] entries = {ByteBuffer.allocate(6).put("foobar".getBytes()).flip()};
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }

        byte[] valueAfterAppend;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            valueAfterAppend = tr.get(volume.computeMutationTriggerKey()).join();
            assertNotNull(valueAfterAppend);
        }

        Versionstamp versionstamp = appendResult.getVersionstampedKeys()[0];
        KeyEntry keyEntry = new KeyEntry(versionstamp, ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip());
        UpdateResult updateResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            updateResult = volume.update(session, keyEntry);
            tr.commit().join();
        }
        updateResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] valueAfterUpdate = tr.get(volume.computeMutationTriggerKey()).join();
            assertNotNull(valueAfterUpdate);
            assertNotEquals(valueAfterAppend, valueAfterUpdate);
        }
    }

    @Test
    void shouldRecordAppendAndUpdateOperationsInChangeLog() throws IOException, KeyNotFoundException {
        byte[] original = new byte[]{1, 2, 3};
        ByteBuffer[] entries = {ByteBuffer.wrap(original)};

        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp versionstamp = appendResult.getVersionstampedKeys()[0];

        byte[] updated = new byte[]{4, 5, 6, 7};
        KeyEntry keyEntry = new KeyEntry(versionstamp, ByteBuffer.wrap(updated));
        UpdateResult updateResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            updateResult = volume.update(session, keyEntry);
            tr.commit().join();
        }
        updateResult.complete();

        List<SegmentAnalysis> analysis = volume.analyze();
        long segmentId = analysis.getFirst().segmentId();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(Subspaces.CHANGELOG_SUBSPACE));
            List<KeyValue> changeLogEntries = tr.getRange(range).asList().join();

            assertEquals(2, changeLogEntries.size());

            long previousLogSeq = -1;
            for (int i = 0; i < changeLogEntries.size(); i++) {
                KeyValue kv = changeLogEntries.get(i);
                Tuple keyTuple = subspace.unpack(kv.getKey());
                Tuple valueTuple = Tuple.fromBytes(kv.getValue());

                long logSeq = keyTuple.getLong(1);
                assertTrue(logSeq > previousLogSeq);
                previousLogSeq = logSeq;

                long rawParentOpKind = keyTuple.getLong(2);
                ParentOperationKind parentOpKind = ParentOperationKind.valueOf((byte) rawParentOpKind);
                assertEquals(ParentOperationKind.LIFECYCLE, parentOpKind);

                long rawOpKind = keyTuple.getLong(3);
                OperationKind operationKind = OperationKind.valueOf((byte) rawOpKind);
                Versionstamp changeLogVersionstamp = keyTuple.getVersionstamp(4);

                assertEquals(versionstamp, changeLogVersionstamp);

                if (i == 0) {
                    // First entry: APPEND for the original data
                    assertEquals(OperationKind.APPEND, operationKind);
                    assertEquals(segmentId, valueTuple.getLong(0));
                    assertEquals(0, valueTuple.getLong(1));
                    assertEquals(original.length, valueTuple.getLong(2));
                    assertEquals(stashVolumeSyncerPrefix.asLong(), valueTuple.getLong(3));
                } else {
                    // Second entry: UPDATE containing both prev and current metadata
                    assertEquals(OperationKind.UPDATE, operationKind);
                    // Current metadata
                    assertEquals(segmentId, valueTuple.getLong(0));
                    assertEquals(original.length, valueTuple.getLong(1));
                    assertEquals(updated.length, valueTuple.getLong(2));
                    // Previous metadata
                    assertEquals(segmentId, valueTuple.getLong(3));
                    assertEquals(0, valueTuple.getLong(4));
                    assertEquals(original.length, valueTuple.getLong(5));
                    assertEquals(stashVolumeSyncerPrefix.asLong(), valueTuple.getLong(6));
                }
            }
        }
    }

    @Test
    void shouldDeleteEntriesByVersionstampedEntry() throws IOException {
        // Behavior: Appends two entries, retrieves their key and metadata via getRange, deletes them
        // using deleteByVersionstampedEntry, and verifies entries are no longer readable.
        ByteBuffer[] entries = {
                ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
        };
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        List<VersionstampedEntry> versionstampedEntries = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                versionstampedEntries.add(new VersionstampedEntry(volumeEntry.key(), EntryMetadata.decode(volumeEntry.metadata())));
            }
        }
        assertEquals(2, versionstampedEntries.size());

        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            deleteResult = volume.deleteByVersionstampedEntry(session, versionstampedEntries.toArray(new VersionstampedEntry[0]));
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (Versionstamp versionstamp : versionstampedKeys) {
                assertNull(volume.get(session, versionstamp));
            }
        }
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenDeletingByVersionstampedEntryWithoutEntries() {
        // Behavior: Calling deleteByVersionstampedEntry with zero entries throws IllegalArgumentException.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.deleteByVersionstampedEntry(session));
        }
    }

    @Test
    void shouldInsertEntriesWithKnownVersionstamps() throws IOException, VersionstampAlreadyExistsException {
        // Behavior: Inserts two entries with caller-provided complete versionstamps,
        // reads them back, and verifies the data matches.
        Versionstamp key1 = TestUtil.generateVersionstamp(0);
        Versionstamp key2 = TestUtil.generateVersionstamp(1);

        byte[] data1 = "hello".getBytes();
        byte[] data2 = "world".getBytes();

        KeyEntry[] pairs = {
                new KeyEntry(key1, ByteBuffer.wrap(data1)),
                new KeyEntry(key2, ByteBuffer.wrap(data2)),
        };

        InsertResult insertResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            insertResult = volume.insert(session, pairs);
            tr.commit().join();
        }
        insertResult.complete();

        assertEquals(2, insertResult.entries().length);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            ByteBuffer result1 = volume.get(session, key1);
            assertNotNull(result1);
            assertEquals(ByteBuffer.wrap(data1), result1);

            ByteBuffer result2 = volume.get(session, key2);
            assertNotNull(result2);
            assertEquals(ByteBuffer.wrap(data2), result2);
        }
    }

    @Test
    void shouldThrowExceptionWhenInsertingDuplicateVersionstamp() throws IOException, VersionstampAlreadyExistsException {
        // Behavior: Inserting an entry with a versionstamp that already exists throws VersionstampAlreadyExistsException.
        Versionstamp key = TestUtil.generateVersionstamp(0);
        byte[] data = "hello".getBytes();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            volume.insert(session, new KeyEntry(key, ByteBuffer.wrap(data)));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(VersionstampAlreadyExistsException.class,
                    () -> volume.insert(session, new KeyEntry(key, ByteBuffer.wrap("other".getBytes()))));
        }
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenInsertingEmptyPairs() {
        // Behavior: Calling insert with zero pairs throws IllegalArgumentException.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.insert(session));
        }
    }

    @Test
    void shouldThrowEntrySizeExceedsLimitExceptionOnInsert() {
        // Behavior: Inserting an entry larger than ENTRY_SIZE_LIMIT throws EntrySizeExceedsLimitException.
        Versionstamp key = TestUtil.generateVersionstamp(0);
        ByteBuffer oversizedEntry = randomBytes(Volume.ENTRY_SIZE_LIMIT + 1);
        KeyEntry pair = new KeyEntry(key, oversizedEntry);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(EntrySizeExceedsLimitException.class, () -> volume.insert(session, pair));
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionOnInsert() {
        // Behavior: Inserting into a read-only volume throws VolumeReadOnlyException.
        volume.setStatus(VolumeStatus.READONLY);

        Versionstamp key = TestUtil.generateVersionstamp(0);
        KeyEntry pair = new KeyEntry(key, ByteBuffer.wrap("data".getBytes()));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.insert(session, pair));
        }
    }

    @Test
    void shouldInsertAndDeleteEntries() throws IOException, VersionstampAlreadyExistsException {
        // Behavior: Inserts entries with known versionstamps, deletes them, and verifies they return null on read.
        Versionstamp key1 = TestUtil.generateVersionstamp(0);
        Versionstamp key2 = TestUtil.generateVersionstamp(1);

        KeyEntry[] pairs = {
                new KeyEntry(key1, ByteBuffer.wrap("hello".getBytes())),
                new KeyEntry(key2, ByteBuffer.wrap("world".getBytes())),
        };

        InsertResult insertResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            insertResult = volume.insert(session, pairs);
            tr.commit().join();
        }
        insertResult.complete();

        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            deleteResult = volume.delete(session, key1, key2);
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertNull(volume.get(session, key1));
            assertNull(volume.get(session, key2));
        }
    }

    @Test
    void shouldInsertAndUpdateEntries() throws IOException, VersionstampAlreadyExistsException, KeyNotFoundException {
        // Behavior: Inserts entries with known versionstamps, updates them with new data,
        // and verifies the updated data is readable.
        Versionstamp key = TestUtil.generateVersionstamp(0);
        byte[] originalData = "original".getBytes();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            InsertResult insertResult = volume.insert(session, new KeyEntry(key, ByteBuffer.wrap(originalData)));
            tr.commit().join();
            insertResult.complete();
        }

        byte[] updatedData = "updated".getBytes();
        UpdateResult updateResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            updateResult = volume.update(session, new KeyEntry(key, ByteBuffer.wrap(updatedData)));
            tr.commit().join();
        }
        updateResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            ByteBuffer result = volume.get(session, key);
            assertNotNull(result);
            assertEquals(ByteBuffer.wrap(updatedData), result);
        }
    }

    @Test
    void shouldUpdateSegmentCardinalityAndUsedBytesOnInsert() throws IOException, VersionstampAlreadyExistsException {
        // Behavior: Inserting entries with known versionstamps correctly updates segment
        // cardinality and used bytes, with a zero garbage percentage.
        byte[] data = "hello".getBytes(); // 5 bytes each
        int entryCount = 3;

        KeyEntry[] entries = new KeyEntry[entryCount];
        for (int i = 0; i < entryCount; i++) {
            entries[i] = new KeyEntry(TestUtil.generateVersionstamp(i), ByteBuffer.wrap(data));
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            InsertResult insertResult = volume.insert(session, entries);
            tr.commit().join();
            insertResult.complete();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> analysis = volume.analyze(tr);
            assertEquals(1, analysis.size());

            SegmentAnalysis segmentAnalysis = analysis.getFirst();
            assertEquals(entryCount, segmentAnalysis.cardinality());
            assertEquals((long) entryCount * data.length, segmentAnalysis.usedBytes());
            assertEquals(0.0, segmentAnalysis.garbagePercentage());
            assertEquals(segmentAnalysis.size() - segmentAnalysis.usedBytes(), segmentAnalysis.freeBytes());
        }
    }

    @Test
    void shouldUpdateSegmentCardinalityWhenDeletingByVersionstampedEntry() throws IOException {
        // Behavior: Deleting entries correctly decreases segment cardinality and used bytes.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            volume.append(session, getEntries(10));
            tr.commit().join();
        }

        List<VersionstampedEntry> versionstampedEntries = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                versionstampedEntries.add(new VersionstampedEntry(volumeEntry.key(), EntryMetadata.decode(volumeEntry.metadata())));
            }
        }

        // Delete entries at indices 3-6 (4 entries)
        VersionstampedEntry[] toDelete = versionstampedEntries.subList(3, 7).toArray(new VersionstampedEntry[0]);
        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            deleteResult = volume.deleteByVersionstampedEntry(session, toDelete);
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> analysis = volume.analyze(tr);
            SegmentAnalysis segmentAnalysis = analysis.getFirst();
            assertTrue(segmentAnalysis.garbagePercentage() > 0);
            long garbageBytes = segmentAnalysis.size() - segmentAnalysis.freeBytes() - segmentAnalysis.usedBytes();
            assertEquals(40, garbageBytes); // Deleted 4 items, test entry size is 10.
        }
    }

    @Test
    void shouldUpdateEntriesByVersionstampedEntryUpdate() throws IOException {
        // Behavior: Appends two entries, retrieves their versionstamps and metadata, updates them with new data
        // using updateByVersionstampedEntryUpdate, and verifies the new data is readable via volume.get.
        Versionstamp[] versionstampedKeys;

        {
            ByteBuffer[] entries = {
                    ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                    ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
            };
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                result = volume.append(session, entries);
                tr.commit().join();
            }
            versionstampedKeys = result.getVersionstampedKeys();
        }

        {
            List<EntryMetadata> metadataList = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                for (VolumeEntry volumeEntry : volume.getRange(session)) {
                    metadataList.add(EntryMetadata.decode(volumeEntry.metadata()));
                }
            }
            assertEquals(2, metadataList.size());

            VersionstampedEntryUpdate[] updates = new VersionstampedEntryUpdate[2];
            updates[0] = new VersionstampedEntryUpdate(versionstampedKeys[0], metadataList.get(0), ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip());
            updates[1] = new VersionstampedEntryUpdate(versionstampedKeys[1], metadataList.get(1), ByteBuffer.allocate(6).put("BARFOO".getBytes()).flip());

            UpdateResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                result = volume.updateByVersionstampedEntryUpdate(session, updates);
                tr.commit().join();
            }
            result.complete();

            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                for (Versionstamp versionstamp : versionstampedKeys) {
                    ByteBuffer buffer = volume.get(session, versionstamp);
                    retrievedEntries.add(buffer);
                }
            }
            assertEquals(ByteBuffer.wrap("FOOBAR".getBytes()), retrievedEntries.get(0));
            assertEquals(ByteBuffer.wrap("BARFOO".getBytes()), retrievedEntries.get(1));
        }
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenUpdatingByVersionstampedEntryUpdateWithoutEntries() {
        // Behavior: Calling updateByVersionstampedEntryUpdate with zero entries throws IllegalArgumentException.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.updateByVersionstampedEntryUpdate(session));
        }
    }

    @Test
    void shouldThrowEntrySizeExceedsLimitExceptionWhenUpdatingByVersionstampedEntryUpdateWithOversizedEntry() throws IOException {
        // Behavior: Updating with an entry larger than ENTRY_SIZE_LIMIT throws EntrySizeExceedsLimitException.
        ByteBuffer smallEntry = ByteBuffer.allocate(10).put("small".getBytes()).flip();
        Versionstamp versionstampedKey;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            AppendResult result = volume.append(session, smallEntry);
            tr.commit().join();
            versionstampedKey = result.getVersionstampedKeys()[0];
        }

        EntryMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = volume.getRange(session);
            VolumeEntry volumeEntry = iterable.iterator().next();
            metadata = EntryMetadata.decode(volumeEntry.metadata());
        }

        ByteBuffer oversizedEntry = randomBytes(Volume.ENTRY_SIZE_LIMIT + 1);
        VersionstampedEntryUpdate update = new VersionstampedEntryUpdate(versionstampedKey, metadata, oversizedEntry);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(EntrySizeExceedsLimitException.class, () -> volume.updateByVersionstampedEntryUpdate(session, update));
        }
    }

    @Test
    void shouldUpdateSegmentCardinalityWhenUpdatingByVersionstampedEntryUpdate() throws IOException {
        // Behavior: When entries move to new segments during the update, cardinality is correctly adjusted —
        // old segments get cardinality 0, new segments get the entries.
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        ByteBuffer[] entries = new ByteBuffer[(int) numIterations];
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < numIterations; i++) {
                entries[i] = randomBytes((int) bufferSize);
            }
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        List<EntryMetadata> metadataList = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                metadataList.add(EntryMetadata.decode(volumeEntry.metadata()));
            }
        }

        VersionstampedEntryUpdate[] updates = new VersionstampedEntryUpdate[metadataList.size()];
        for (int i = 0; i < metadataList.size(); i++) {
            updates[i] = new VersionstampedEntryUpdate(versionstampedKeys[i], metadataList.get(i), randomBytes((int) bufferSize));
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            UpdateResult updateResult = volume.updateByVersionstampedEntryUpdate(session, updates);
            tr.commit().join();
            updateResult.complete();
        }

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        assertEquals(4, segmentAnalysis.size());

        long staleCount = segmentAnalysis.stream().filter(a -> a.cardinality() == 0).count();
        long liveCount = segmentAnalysis.stream().filter(a -> a.cardinality() > 0).count();
        assertEquals(2, staleCount);
        assertEquals(2, liveCount);

        for (SegmentAnalysis analysis : segmentAnalysis) {
            if (analysis.cardinality() > 0) {
                assertEquals(10, analysis.cardinality());
            }
        }
    }

    @Test
    void shouldPreserveVersionstampKeysWhenUpdatingByVersionstampedEntryUpdate() throws IOException {
        // Behavior: After the update, the same versionstamp keys still point to the updated entries
        // (the keys don't change, only the data and metadata do).
        ByteBuffer[] originalEntries = {
                ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
        };
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, originalEntries);
            tr.commit().join();
        }
        Versionstamp[] originalKeys = appendResult.getVersionstampedKeys();

        List<EntryMetadata> metadataList = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                metadataList.add(EntryMetadata.decode(volumeEntry.metadata()));
            }
        }

        VersionstampedEntryUpdate[] updates = new VersionstampedEntryUpdate[2];
        updates[0] = new VersionstampedEntryUpdate(originalKeys[0], metadataList.get(0), ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip());
        updates[1] = new VersionstampedEntryUpdate(originalKeys[1], metadataList.get(1), ByteBuffer.allocate(6).put("BARFOO".getBytes()).flip());

        UpdateResult updateResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            updateResult = volume.updateByVersionstampedEntryUpdate(session, updates);
            tr.commit().join();
        }
        updateResult.complete();

        UpdatedEntry[] updatedEntries = updateResult.entries();
        assertEquals(2, updatedEntries.length);
        assertEquals(originalKeys[0], updatedEntries[0].versionstamp());
        assertEquals(originalKeys[1], updatedEntries[1].versionstamp());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            ByteBuffer first = volume.get(session, originalKeys[0]);
            assertEquals(ByteBuffer.wrap("FOOBAR".getBytes()), first);
            ByteBuffer second = volume.get(session, originalKeys[1]);
            assertEquals(ByteBuffer.wrap("BARFOO".getBytes()), second);
        }
    }

    @Test
    void shouldRecordUpdateOperationInChangeLog_updateByVersionstampedEntryUpdate() throws IOException {
        // Behavior: Appends entries via Volume, updates some via Volume.updateByVersionstampedEntryUpdate,
        // and verifies the ChangeLog contains UPDATE entries with correct before/after coordinates,
        // versionstamps, and prefix.
        ByteBuffer[] entries = getEntries(3);
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        List<EntryMetadata> metadataList = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                metadataList.add(EntryMetadata.decode(volumeEntry.metadata()));
            }
        }
        assertEquals(3, metadataList.size());

        EntryMetadata prevMeta0 = metadataList.get(0);
        EntryMetadata prevMeta1 = metadataList.get(1);
        VersionstampedEntryUpdate[] updates = {
                new VersionstampedEntryUpdate(versionstampedKeys[0], prevMeta0, ByteBuffer.allocate(10).put("updated-00".getBytes()).flip()),
                new VersionstampedEntryUpdate(versionstampedKeys[1], prevMeta1, ByteBuffer.allocate(10).put("updated-01".getBytes()).flip()),
        };
        UpdateResult updateResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            updateResult = volume.updateByVersionstampedEntryUpdate(session, updates);
            tr.commit().join();
        }
        updateResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            List<ChangeLogEntry> changeLogEntries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                changeLogEntries.add(entry);
            }

            // 3 APPENDs + 2 UPDATEs = 5 entries
            assertEquals(5, changeLogEntries.size());

            for (int i = 0; i < 3; i++) {
                assertEquals(OperationKind.APPEND, changeLogEntries.get(i).getKind());
            }

            EntryMetadata[] prevMetadatas = {prevMeta0, prevMeta1};
            UpdatedEntry[] updatedEntries = updateResult.entries();

            for (int i = 3; i < 5; i++) {
                ChangeLogEntry updateEntry = changeLogEntries.get(i);
                assertEquals(OperationKind.UPDATE, updateEntry.getKind());

                assertTrue(updateEntry.hasBefore());
                assertTrue(updateEntry.hasAfter());

                int updatedIndex = i - 3;
                EntryMetadata prevMetadata = prevMetadatas[updatedIndex];
                EntryMetadata newMetadata = updatedEntries[updatedIndex].metadata();

                ChangeLogCoordinate before = updateEntry.getBefore().orElseThrow();
                assertEquals(prevMetadata.segmentId(), before.segmentId());
                assertEquals(prevMetadata.position(), before.position());
                assertEquals(prevMetadata.length(), before.length());

                ChangeLogCoordinate after = updateEntry.getAfter().orElseThrow();
                assertEquals(newMetadata.segmentId(), after.segmentId());
                assertEquals(newMetadata.position(), after.position());
                assertEquals(newMetadata.length(), after.length());

                assertEquals(versionstampedKeys[updatedIndex], updateEntry.getVersionstamp());

                assertEquals(stashVolumeSyncerPrefix.asLong(), updateEntry.getPrefix());
            }
        }
    }

    @Test
    void shouldRecordDeleteOperationInChangeLog_deleteByVersionstampedEntry() throws IOException {
        // Behavior: Appends entries via Volume, deletes some via Volume.deleteByVersionstampedEntry,
        // and verifies the ChangeLog contains DELETE entries with correct metadata, versionstamps,
        // and coordinates.
        ByteBuffer[] entries = getEntries(3);
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        List<VersionstampedEntry> versionstampedEntries = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                versionstampedEntries.add(new VersionstampedEntry(volumeEntry.key(), EntryMetadata.decode(volumeEntry.metadata())));
            }
        }
        assertEquals(3, versionstampedEntries.size());

        // Delete the first 2 entries via Volume.deleteByVersionstampedEntry
        VersionstampedEntry[] toDelete = versionstampedEntries.subList(0, 2).toArray(new VersionstampedEntry[0]);
        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            deleteResult = volume.deleteByVersionstampedEntry(session, toDelete);
            tr.commit().join();
        }
        deleteResult.complete();

        // Read ChangeLog and verify DELETE entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            List<ChangeLogEntry> changeLogEntries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                changeLogEntries.add(entry);
            }

            // 3 APPENDs + 2 DELETEs = 5 entries
            assertEquals(5, changeLogEntries.size());

            // First 3 are APPENDs
            for (int i = 0; i < 3; i++) {
                assertEquals(OperationKind.APPEND, changeLogEntries.get(i).getKind());
            }

            // Last 2 are DELETEs
            for (int i = 3; i < 5; i++) {
                ChangeLogEntry deleteEntry = changeLogEntries.get(i);
                assertEquals(OperationKind.DELETE, deleteEntry.getKind());

                // DELETE: before present, after absent
                assertTrue(deleteEntry.hasBefore());
                assertFalse(deleteEntry.hasAfter());

                // Verify coordinates match the deleted entry's metadata
                int deletedIndex = i - 3;
                EntryMetadata deletedMetadata = toDelete[deletedIndex].metadata();
                ChangeLogCoordinate before = deleteEntry.getBefore().orElseThrow();
                assertEquals(deletedMetadata.segmentId(), before.segmentId());
                assertEquals(deletedMetadata.position(), before.position());
                assertEquals(deletedMetadata.length(), before.length());

                // Verify versionstamp matches the original appended entry
                assertEquals(versionstampedKeys[deletedIndex], deleteEntry.getVersionstamp());

                // Verify prefix
                assertEquals(stashVolumeSyncerPrefix.asLong(), deleteEntry.getPrefix());
            }
        }
    }

    @Test
    void shouldFindCorrectSegmentPositionWithMultiplePrefixes() throws IOException {
        // Behavior: When multiple prefixes write to the same segment in an interleaved order, closing
        // and reopening the Volume must recover the correct (highest) write position, and subsequent
        // appends must not corrupt previously written data.

        Prefix prefixA = new Prefix("prefix-a");
        Prefix prefixB = new Prefix("prefix-b");

        ByteBuffer entryA1 = getEntry();
        ByteBuffer entryB1 = getEntry();
        ByteBuffer entryA2 = getEntry();

        List<Versionstamp> allKeys = new ArrayList<>();

        // Tx1: Append entry to prefix A (lands at position 0)
        {
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefixA);
                result = volume.append(session, entryA1);
                tr.commit().join();
            }
            allKeys.add(result.getVersionstampedKeys()[0]);
        }

        // Tx2: Append entry to prefix B (lands after A's entry)
        {
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefixB);
                result = volume.append(session, entryB1);
                tr.commit().join();
            }
            allKeys.add(result.getVersionstampedKeys()[0]);
        }

        // Tx3: Append entry to prefix A again (lands at the highest position)
        {
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefixA);
                result = volume.append(session, entryA2);
                tr.commit().join();
            }
            allKeys.add(result.getVersionstampedKeys()[0]);
        }

        List<SegmentAnalysis> analysis = volume.analyze();
        assertEquals(1, analysis.size());
        long segmentId = analysis.getFirst().segmentId();

        // Close the volume
        volume.close();

        // Verify the tail pointer reflects the last (third) entry written
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentTailPointer tailPointer = SegmentSubspaceUtil.locateTailPointer(tr, volume.getConfig().subspace(), segmentId);
            assertEquals(allKeys.get(2), tailPointer.versionstamp());
            assertEquals(entryA1.capacity() + entryB1.capacity(), tailPointer.position());
            assertEquals(entryA2.capacity(), tailPointer.length());

            long expectedNextPosition = entryA1.capacity() + entryB1.capacity() + entryA2.capacity();
            assertEquals(expectedNextPosition, tailPointer.nextPosition());
        }

        // Reopen the volume
        Volume reopenedVolume = service.newVolume(volume.getConfig());

        // Append a new entry to prefix A on the reopened volume
        ByteBuffer entryA3 = getEntry();
        {
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefixA);
                result = reopenedVolume.append(session, entryA3);
                tr.commit().join();
            }
            allKeys.add(result.getVersionstampedKeys()[0]);
        }

        // Verify all 4 entries are readable and contain correct data
        ByteBuffer[] expectedEntries = {entryA1, entryB1, entryA2, entryA3};
        Prefix[] prefixes = {prefixA, prefixB, prefixA, prefixA};

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < allKeys.size(); i++) {
                VolumeSession session = new VolumeSession(tr, prefixes[i]);
                ByteBuffer retrieved = reopenedVolume.get(session, allKeys.get(i));
                assertNotNull(retrieved, "Entry " + i + " should be readable");
                assertEquals(expectedEntries[i].rewind(), retrieved,
                        "Entry " + i + " data should match");
            }
        }
    }

    @Test
    void shouldPreserveOtherPrefixDataWhenClearingPrefix() throws IOException {
        // Behavior: When clearPrefix is called for one prefix, entries, cardinality, and
        // usedBytes belonging to a different prefix in the same segment are preserved.
        Prefix prefixA = new Prefix("prefix-a");
        Prefix prefixB = new Prefix("prefix-b");

        ByteBuffer[] entriesA = getEntries(3);
        ByteBuffer[] entriesB = getEntries(3);

        // Append entries under both prefixes
        AppendResult resultB;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession sessionA = new VolumeSession(tr, prefixA);
            volume.append(sessionA, entriesA);

            VolumeSession sessionB = new VolumeSession(tr, prefixB);
            resultB = volume.append(sessionB, entriesB);
            tr.commit().join();
        }

        List<SegmentAnalysis> analysis = volume.analyze();
        assertEquals(1, analysis.size());
        long segmentId = analysis.getFirst().segmentId();

        // Clear prefix A
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession sessionA = new VolumeSession(tr, prefixA);
            volume.clearPrefix(sessionA);
            tr.commit().join();
        }

        // Verify prefix B's ENTRY_SUBSPACE entries are intact
        byte[] entryPrefixB = volume.getConfig().subspace().pack(
                Tuple.from(ENTRY_SUBSPACE, prefixB.asBytes()));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> entryEntries = tr.getRange(Range.startsWith(entryPrefixB)).asList().join();
            assertEquals(3, entryEntries.size(),
                    "Prefix B's ENTRY_SUBSPACE entries should be preserved");
        }

        // Verify prefix B's entries are still readable
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession sessionB = new VolumeSession(tr, prefixB);
            for (Versionstamp key : resultB.getVersionstampedKeys()) {
                ByteBuffer retrieved = volume.get(sessionB, key);
                assertNotNull(retrieved, "Prefix B entry should still be readable");
            }
        }

        // Verify prefix B's cardinality and usedBytes are intact
        VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession sessionB = new VolumeSession(tr, prefixB);
            SegmentMetadata metadata = new SegmentMetadata(volumeSubspace, segmentId);
            assertEquals(3, metadata.cardinality(sessionB),
                    "Prefix B's cardinality should be preserved");
            assertTrue(metadata.usedBytes(sessionB) > 0,
                    "Prefix B's usedBytes should be preserved");
        }

        // Verify prefix A's ENTRY_SUBSPACE entries are gone
        byte[] entryPrefixA = volume.getConfig().subspace().pack(
                Tuple.from(ENTRY_SUBSPACE, prefixA.asBytes()));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> entryEntries = tr.getRange(Range.startsWith(entryPrefixA)).asList().join();
            assertTrue(entryEntries.isEmpty(),
                    "Prefix A's ENTRY_SUBSPACE entries should be cleared");
        }

        // Verify prefix A's cardinality is gone (key should not exist)
        byte[] cardinalityKeyA = volume.getConfig().subspace().pack(
                Tuple.from(SEGMENT_STATS_SUBSPACE, segmentId, prefixA.asBytes(), CARDINALITY));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = tr.get(cardinalityKeyA).join();
            assertNull(value, "Prefix A's cardinality should be cleared");
        }
    }

    @Test
    void shouldWriteNewReverseIndexAfterUpdate() throws IOException, KeyNotFoundException {
        // Behavior: After updating an entry via volume.update, the ENTRY_METADATA_SUBSPACE reverse
        // index contains the new metadata key pointing back to the versionstamp.
        ByteBuffer[] entries = {ByteBuffer.allocate(6).put("foobar".getBytes()).flip()};
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp key = appendResult.getVersionstampedKeys()[0];

        // Update the entry with new data
        KeyEntry[] updateEntries = {new KeyEntry(key, ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip())};
        UpdateResult updateResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            updateResult = volume.update(session, updateEntries);
            tr.commit().join();
        }
        updateResult.complete();

        // Read the new EntryMetadata from ENTRY_SUBSPACE
        byte[] newEncodedMetadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
            byte[] packedKey = volumeSubspace.packEntryKey(stashVolumeSyncerPrefix, key);
            newEncodedMetadata = tr.get(packedKey).join();
            assertNotNull(newEncodedMetadata, "Updated entry metadata should exist in ENTRY_SUBSPACE");
        }

        // Verify the reverse index (ENTRY_METADATA_SUBSPACE) has an entry for the new metadata
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
            byte[] reverseKey = volumeSubspace.packEntryMetadataKey(newEncodedMetadata);
            byte[] reverseValue = tr.get(reverseKey).join();
            assertNotNull(reverseValue, "ENTRY_METADATA_SUBSPACE should contain the new reverse index entry");

            Versionstamp decoded = VersionstampUtil.decodeVersionstampedValue(reverseValue);
            assertEquals(key, decoded, "Reverse index should point back to the original versionstamp");
        }
    }

    @Test
    void shouldTrackUsedBytesCorrectlyAfterSameSegmentUpdate() throws IOException, KeyNotFoundException {
        // Behavior: After updating an entry with different-sized data in the same segment,
        // usedBytes correctly reflects the new size (old bytes subtracted, new bytes added).
        byte[] smallData = "hi".getBytes(); // 2 bytes
        ByteBuffer[] entries = {ByteBuffer.allocate(smallData.length).put(smallData).flip()};

        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp key = appendResult.getVersionstampedKeys()[0];

        long usedBytesBeforeUpdate;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> analysis = volume.analyze(tr);
            assertEquals(1, analysis.size());
            usedBytesBeforeUpdate = analysis.getFirst().usedBytes();
            assertEquals(smallData.length, usedBytesBeforeUpdate);
        }

        // Update with larger data
        byte[] largerData = "hello world!".getBytes(); // 12 bytes
        KeyEntry[] updateEntries = {new KeyEntry(key, ByteBuffer.allocate(largerData.length).put(largerData).flip())};
        UpdateResult updateResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            updateResult = volume.update(session, updateEntries);
            tr.commit().join();
        }
        updateResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> analysis = volume.analyze(tr);
            assertEquals(1, analysis.size());
            SegmentAnalysis segmentAnalysis = analysis.getFirst();
            assertEquals(largerData.length, segmentAnalysis.usedBytes(),
                    "usedBytes should reflect the new entry size after same-segment update");
        }
    }

    @Test
    void shouldAllowDeleteByVersionstampedEntryAfterUpdate() throws IOException, KeyNotFoundException {
        // Behavior: After updating an entry, deleteByVersionstampedEntry with the new metadata
        // successfully deletes the entry.
        ByteBuffer[] entries = {ByteBuffer.allocate(6).put("foobar".getBytes()).flip()};
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp key = appendResult.getVersionstampedKeys()[0];

        // Update the entry
        KeyEntry[] updateEntries = {new KeyEntry(key, ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip())};
        UpdateResult updateResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            updateResult = volume.update(session, updateEntries);
            tr.commit().join();
        }
        updateResult.complete();

        // Read the updated VersionstampedEntry
        VersionstampedEntry updatedEntry;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            List<VersionstampedEntry> versionstampedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                versionstampedEntries.add(new VersionstampedEntry(volumeEntry.key(), EntryMetadata.decode(volumeEntry.metadata())));
            }
            assertEquals(1, versionstampedEntries.size());
            updatedEntry = versionstampedEntries.getFirst();
        }

        // Delete by the updated entry
        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            deleteResult = volume.deleteByVersionstampedEntry(session, updatedEntry);
            tr.commit().join();
        }
        deleteResult.complete();

        // Verify the entry is no longer readable
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertNull(volume.get(session, key), "Entry should be deleted");
        }
    }

    @Test
    void shouldThrowClosedVolumeExceptionOnAppendAfterClose() {
        // Behavior: append on a closed volume must throw ClosedVolumeException immediately
        volume.close();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(ClosedVolumeException.class, () -> volume.append(session, getEntries(1)));
        }
    }

    @Test
    void shouldThrowClosedVolumeExceptionOnUpdateAfterClose() throws IOException {
        // Behavior: update on a closed volume must throw ClosedVolumeException immediately
        Versionstamp key;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            AppendResult result = volume.append(session, getEntries(1));
            tr.commit().join();
            key = result.getVersionstampedKeys()[0];
        }

        volume.close();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            KeyEntry pair = new KeyEntry(key, getEntry());
            assertThrows(ClosedVolumeException.class, () -> volume.update(session, pair));
        }
    }

    @Test
    void shouldThrowClosedVolumeExceptionOnInsertAfterClose() {
        // Behavior: insert on a closed volume must throw ClosedVolumeException immediately
        volume.close();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            Versionstamp key = TestUtil.generateVersionstamp(0);
            KeyEntry pair = new KeyEntry(key, getEntry());
            assertThrows(ClosedVolumeException.class, () -> volume.insert(session, pair));
        }
    }

    @Test
    void shouldNotBusySpinWhenClosedDuringAppend() throws InterruptedException {
        // Behavior: a concurrent close must terminate an in-flight append promptly, not busy-spin
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread.ofVirtual().start(() -> {
            started.countDown();
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    try (Transaction tr = context.getFoundationDB().createTransaction()) {
                        VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
                        volume.append(session, getEntries(1));
                        tr.commit().join();
                    }
                }
            } catch (ClosedVolumeException e) {
                // Expected
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                done.countDown();
            }
        });

        started.await();
        Thread.sleep(50);
        volume.close();
        assertTrue(done.await(5, TimeUnit.SECONDS), "Writer did not terminate after volume close");
        assertNull(failure.get(), "Writer failed with unexpected exception");
    }
}