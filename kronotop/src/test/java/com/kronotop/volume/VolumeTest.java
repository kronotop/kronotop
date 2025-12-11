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

package com.kronotop.volume;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.volume.handlers.PackedEntry;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class VolumeTest extends BaseVolumeIntegrationTest {

    @Test
    void shouldAppendEntriesToVolume() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }
        assertEquals(2, result.getVersionstampedKeys().length);
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenAppendingWithoutEntries() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.append(session));
        }
    }

    @Test
    void shouldGetEntriesByVersionstamp() throws IOException {
        ByteBuffer[] entries = getEntries(3);

        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        List<ByteBuffer> retrievedEntries = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            for (Versionstamp versionstamp : versionstampedKeys) {
                ByteBuffer buffer = volume.get(session, versionstamp);
                retrievedEntries.add(buffer);
            }
        }
        for (int i = 0; i < retrievedEntries.size(); i++) {
            assertArrayEquals(entries[i].array(), retrievedEntries.get(i).array());
        }
    }

    @Test
    void shouldDeleteEntriesAndReturnNullOnGet() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            deleteResult = volume.delete(session, versionstampedKeys);
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
    void shouldThrowIllegalArgumentExceptionWhenDeletingWithoutKeys() throws IOException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
                result = volume.update(session, entries);
                tr.commit().join();
            }
            result.complete();

            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
                for (Versionstamp versionstamp : versionstampedKeys) {
                    ByteBuffer buffer = volume.get(session, versionstamp);
                    retrievedEntries.add(buffer);
                }
            }
            for (int i = 0; i < retrievedEntries.size(); i++) {
                assertArrayEquals(entries[i].entry().array(), retrievedEntries.get(i).array());
            }
        }
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenUpdatingWithoutEntries() throws IOException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.update(session));
        }
    }

    @Test
    void shouldThrowEntrySizeExceedsLimitExceptionWhenUpdatingWithOversizedEntry() throws IOException {
        Versionstamp versionstampedKey;

        // First, append a small entry to get a key
        ByteBuffer smallEntry = ByteBuffer.allocate(10).put("small".getBytes()).flip();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, smallEntry);
            tr.commit().join();
            versionstampedKey = result.getVersionstampedKeys()[0];
        }

        // Try to update with an oversized entry
        ByteBuffer oversizedEntry = randomBytes(Volume.ENTRY_SIZE_LIMIT + 1);
        KeyEntry oversizedPair = new KeyEntry(versionstampedKey, oversizedEntry);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(EntrySizeExceedsLimitException.class, () -> volume.update(session, oversizedPair));
        }
    }

    @Test
    void shouldFlushWithoutError() {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertDoesNotThrow(() -> volume.append(session, entries));
            tr.commit().join();
        }
        assertDoesNotThrow(() -> volume.flush());
    }

    @Test
    void shouldCloseWithoutError() {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
                for (Versionstamp versionstamp : versionstampedKeys) {
                    ByteBuffer buffer = reopenedVolume.get(session, versionstamp);
                    retrievedEntries.add(buffer);
                }
            }
            for (int i = 0; i < retrievedEntries.size(); i++) {
                assertArrayEquals(entries[i].array(), retrievedEntries.get(i).array());
            }
        }
    }

    @Test
    void shouldDetermineSegmentWritePositionFromPersistedMetadata() throws IOException {
        byte[] data = new byte[]{0x01, 0x02, 0x03};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            volume.append(session, ByteBuffer.wrap(data));
            tr.commit().join();
        }
        List<SegmentAnalysis> analysis = volume.analyze();
        assertEquals(1, analysis.size());

        volume.close();

        SegmentAnalysis segmentAnalysis = analysis.getFirst();
        long position = SegmentUtil.findNextPosition(context, volume.getConfig().subspace(), segmentAnalysis.segmentId());
        assertEquals(data.length, position);
    }

    @Test
    void shouldWriteNewEntriesAfterReopeningVolume() throws IOException {
        List<Versionstamp> versionstampedKeys;

        ByteBuffer[] firstEntries = getEntries(2);
        {
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
                result = reopenedVolume.append(session, secondEntries);
                tr.commit().join();
            }
            versionstampedKeys.addAll(Arrays.stream(result.getVersionstampedKeys()).toList());

            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
                for (Versionstamp versionstamp : versionstampedKeys) {
                    ByteBuffer buffer = reopenedVolume.get(session, versionstamp);
                    retrievedEntries.add(buffer);
                }
            }

            for (int i = 0; i < firstEntries.length; i++) {
                assertArrayEquals(firstEntries[i].array(), retrievedEntries.get(i).array());
            }

            for (int i = 0; i < secondEntries.length; i++) {
                assertArrayEquals(secondEntries[i].array(), retrievedEntries.get(i).array());
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
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
                    VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            for (Map.Entry<Versionstamp, ByteBuffer> entry : pairs.entrySet()) {
                ByteBuffer buffer = volume.get(session, entry.getKey());
                assertArrayEquals(entry.getValue().array(), buffer.array());
            }
        }
    }

    @Test
    void shouldAnalyzeSegmentsCorrectly() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
                assertEquals(0.0, analysis.garbageRatio());
                assertEquals(segmentSize, analysis.size());
                assertEquals(analysis.freeBytes(), analysis.size() - analysis.usedBytes());
                assertTrue(analysis.cardinality() > 0);
                cardinality += analysis.cardinality();
            }
            assertEquals(numIterations, cardinality);
        }
    }

    @Test
    void shouldCalculateGarbageRatioAfterDeletingEntries() throws IOException {
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            appendResult = volume.append(session, getEntries(10));
            tr.commit().join();
        }

        DeleteResult deleteResult;
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();
        Versionstamp[] keys = Arrays.copyOfRange(versionstampedKeys, 3, 7);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            deleteResult = volume.delete(session, keys);
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> analysis = volume.analyze(tr);
            SegmentAnalysis segmentAnalysis = analysis.getFirst();
            assertTrue(segmentAnalysis.garbageRatio() > 0);
            long garbageBytes = segmentAnalysis.size() - segmentAnalysis.freeBytes() - segmentAnalysis.usedBytes();
            assertEquals(40, garbageBytes); // We deleted 4 items and the test entry size is 10.
        }
    }

    @Test
    void shouldThrowEntrySizeExceedsLimitExceptionWhenEntryTooLarge() {
        ByteBuffer oversizedEntry = randomBytes(Volume.ENTRY_SIZE_LIMIT + 1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(EntrySizeExceedsLimitException.class, () -> volume.append(session, oversizedEntry));
        }
    }

    @Test
    void shouldThrowTooManyEntriesExceptionWhenExceedingMaxEntries() {
        ByteBuffer[] entries = getEntries(UserVersion.MAX_VALUE + 1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(TooManyEntriesException.class, () -> volume.append(session, entries));
        }
    }

    @Test
    void shouldThrowTooManyEntriesExceptionWhenExceedingSessionLimit() throws IOException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            UpdateResult updateResult = volume.update(session, pairs);
            tr.commit().join();
            updateResult.complete();
        }

        List<SegmentAnalysis> segmentAnalysis = volume.analyze();
        assertEquals(4, segmentAnalysis.size());

        // Cardinality should be zero for the first two segments.
        for (int i = 0; i < 2; i++) {
            SegmentAnalysis analysis = segmentAnalysis.get(i);
            assertEquals(0, analysis.cardinality());
        }

        // All keys moved to the new segments and the first two segments will be vacuumed.
        for (int i = 2; i < 4; i++) {
            SegmentAnalysis analysis = segmentAnalysis.get(i);
            assertEquals(10, analysis.cardinality());
        }
    }

    @Test
    void shouldVacuumSegmentAndMoveEntries() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        {
            List<SegmentAnalysis> segmentAnalysis = volume.analyze();
            long firstSegmentId = segmentAnalysis.getFirst().segmentId();
            VacuumContext vacuumContext = new VacuumContext(firstSegmentId, new AtomicBoolean());
            volume.vacuumSegment(vacuumContext);
        }

        {
            List<SegmentAnalysis> segmentAnalysis = volume.analyze();
            assertEquals(3, segmentAnalysis.size());

            // Cardinality should be zero for the first segment.
            assertEquals(0, segmentAnalysis.getFirst().cardinality());

            // All keys moved to the new segments.
            for (int i = 1; i < 3; i++) {
                SegmentAnalysis analysis = segmentAnalysis.get(i);
                assertEquals(10, analysis.cardinality());
            }
        }
    }

    @Test
    void shouldGetRangeForFullScan() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        Versionstamp[] retrievedKeys = new Versionstamp[10];
        ByteBuffer[] retrievedEntries = new ByteBuffer[10];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            ByteBuffer expected = entries[i];
            expected.flip();
            ByteBuffer actual = retrievedEntries[i];
            assertArrayEquals(expected.array(), actual.array());
        }
    }

    @Test
    void shouldGetRangeWithLimit() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            ByteBuffer expected = entries[i];
            expected.flip();
            ByteBuffer actual = retrievedEntries[i];
            assertArrayEquals(expected.array(), actual.array());
        }
    }

    @Test
    void shouldGetRangeInReverseOrder() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        // Retrieves the first 5 keys.
        Versionstamp[] retrievedKeys = new Versionstamp[10];
        ByteBuffer[] retrievedEntries = new ByteBuffer[10];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            ByteBuffer expected = entries[i];
            expected.flip();
            ByteBuffer actual = retrievedEntries[i];
            assertArrayEquals(expected.array(), actual.array());
        }
    }

    @Test
    void shouldGetRangeWithCustomSelectors() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] expectedKeys = Arrays.copyOfRange(result.getVersionstampedKeys(), 3, 7);
        ByteBuffer[] expectedEntries = new ByteBuffer[expectedKeys.length];

        VolumeSession sessionWithoutTransaction = new VolumeSession(redisVolumeSyncerPrefix);
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

            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            ByteBuffer expected = expectedEntries[i];
            expected.flip();
            ByteBuffer actual = retrievedEntries[i];
            assertArrayEquals(expected.array(), actual.array());
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
            assertEquals(dataOne, new String(entry.array()));
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefixTwo);
            ByteBuffer entry = volume.get(session, keyOne);
            assertNull(entry);

            entry = volume.get(session, keyTwo);
            assertNotNull(entry);
            assertEquals(dataTwo, new String(entry.array()));
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
                assertEquals(dataTwo, new String(entry.array()));
            }
        }
    }

    @Test
    void shouldGetRangeWithPrefixIsolation() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        {
            int index = 0;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
                Iterable<VolumeEntry> iterable = volume.getRange(session);
                for (VolumeEntry keyEntry : iterable) {
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
                for (VolumeEntry keyEntry : iterable) {
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            volume.clearPrefix(session);
            tr.commit().join();
        }

        {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                SegmentAnalysis analysis = volume.analyze(tr).getFirst();
                byte[] begin = SegmentUtil.prefixOfVolumePrefix(volume.getConfig().subspace(), analysis.segmentId(), redisVolumeSyncerPrefix);
                Range range = Range.startsWith(begin);
                List<KeyValue> survivedEntries = tr.getRange(range).asList().join();
                assertEquals(0, survivedEntries.size());
            }
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            assertArrayEquals(first, firstBuffer.array());

            ByteBuffer secondBuffer = standby.get(session, versionstamps[1]);
            assertArrayEquals(second, secondBuffer.array());
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
            VolumeMetadata.compute(tr, volume.getConfig().subspace(), (volumeMetadata -> {
                assertEquals(VolumeStatus.READONLY, volumeMetadata.getStatus());
            }));
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionWhenAppendingToReadOnlyVolume() throws IOException {
        volume.setStatus(VolumeStatus.READONLY);

        ByteBuffer[] entries = getEntries(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.append(session, entries));
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionWhenDeletingFromReadOnlyVolume() throws IOException {
        ByteBuffer[] entries = getEntries(1);
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        volume.setStatus(VolumeStatus.READONLY);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.delete(session, versionstampedKeys));
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionWhenUpdatingReadOnlyVolume() throws IOException, KeyNotFoundException {
        Versionstamp[] versionstampedKeys;

        {
            ByteBuffer[] entries = {
                    ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                    ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
            };
            AppendResult result;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.update(session, entries));
        }
    }

    @Test
    void shouldThrowVolumeReadOnlyExceptionWhenClearingPrefixOnReadOnlyVolume() throws IOException {
        ByteBuffer[] entries = getEntries(3);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        volume.setStatus(VolumeStatus.READONLY);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
        ConcurrentHashMap<String, String> expected = new ConcurrentHashMap<>();

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
                    VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
                    AppendResult result = volume.append(session, entries);
                    tr.commit().join();
                    Versionstamp[] keys = result.getVersionstampedKeys();
                    for (int i = 0; i < keys.length; i++) {
                        Versionstamp key = keys[i];
                        expected.put(VersionstampUtil.base32HexEncode(key), values.get(i));
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

        ConcurrentHashMap<String, String> result = new ConcurrentHashMap<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (String key : expected.keySet()) {
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
                ByteBuffer buffer = volume.get(session, VersionstampUtil.base32HexDecode(key));
                assertNotNull(buffer);
                result.put(key, new String(buffer.array()));
            }
        }
        assertEquals(expected, result);
    }

    @Test
    void shouldSetNonZeroIdDuringVolumeInitialization() {
        // Test that volume initialization correctly sets a non-zero ID in VolumeMetadata
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata metadata = VolumeMetadata.load(tr, volume.getConfig().subspace());

            // The initialize() method should have set a non-zero ID during volume construction
            assertNotEquals(0, metadata.getVolumeId(), "Volume ID should not be 0 after initialization");

            // Verify the ID is actually a valid integer (not zero)
            assertTrue(metadata.getVolumeId() != 0, "Volume ID must be set to a non-zero value");

            // Test that the ID persists across metadata loads
            VolumeMetadata reloadedMetadata = VolumeMetadata.load(tr, volume.getConfig().subspace());
            assertEquals(metadata.getVolumeId(), reloadedMetadata.getVolumeId(), "Volume ID should be consistent across loads");
        }
    }

    @Test
    void shouldPreventUnnecessarySegmentCreationWhenReopening() throws IOException {
        // Test that openSegments correctly opens existing segments and prevents creating new segments on reopen

        // Create initial volume and append data to force segment creation
        ByteBuffer[] initialEntries = getEntries(5);
        List<Long> segmentsAfterInitialAppend;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            volume.append(session, initialEntries);
            tr.commit().join();
        }

        // Check how many segments exist after initial append
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata metadata = VolumeMetadata.load(tr, volume.getConfig().subspace());
            segmentsAfterInitialAppend = new ArrayList<>(metadata.getSegments());
        }

        assertFalse(segmentsAfterInitialAppend.isEmpty(), "Initial append should create at least one segment");

        // Close the volume
        volume.close();

        // Reopen the volume (this calls openSegments in constructor)
        Volume reopenedVolume = service.newVolume(volume.getConfig());

        // Append new data to the reopened volume
        ByteBuffer[] newEntries = getEntries(3);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            reopenedVolume.append(session, newEntries);
            tr.commit().join();
        }

        List<Long> segmentsAfterReopen;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata metadata = VolumeMetadata.load(tr, reopenedVolume.getConfig().subspace());
            segmentsAfterReopen = new ArrayList<>(metadata.getSegments());
        }

        // Verify that openSegments prevented unnecessary segment creation
        // The segment count should be the same unless the existing segment was full
        // In most cases, no new segments should be created for small appends
        assertEquals(segmentsAfterInitialAppend, segmentsAfterReopen);

        // Verify that we can still read the original data
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);

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
        assertArrayEquals(first, retrievedEntries[0].array());
        assertArrayEquals(second, retrievedEntries[1].array());
        assertArrayEquals(third, retrievedEntries[2].array());
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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

                assertEquals(redisVolumeSyncerPrefix.asLong(), valueTuple.getLong(3));
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstamps = appendResult.getVersionstampedKeys();
        assertEquals(2, versionstamps.length);

        DeleteResult deleteResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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

                assertEquals(redisVolumeSyncerPrefix.asLong(), valueTuple.getLong(3));
            }
        }
    }

    @Test
    void shouldTriggerWatchers_onAppendOperation() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp versionstamp = appendResult.getVersionstampedKeys()[0];

        byte[] updated = new byte[]{4, 5, 6, 7};
        KeyEntry keyEntry = new KeyEntry(versionstamp, ByteBuffer.wrap(updated));
        UpdateResult updateResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
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
                    assertEquals(redisVolumeSyncerPrefix.asLong(), valueTuple.getLong(3));
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
                    assertEquals(redisVolumeSyncerPrefix.asLong(), valueTuple.getLong(6));
                }
            }
        }
    }

    @Test
    void shouldCleanupStaleSegmentsWithZeroCardinality() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        // Fill two segments with data
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        // Verify we have 2 segments
        List<SegmentAnalysis> initialAnalysis = volume.analyze();
        assertEquals(2, initialAnalysis.size());

        // Get the first segment's file path before vacuum
        long firstSegmentId = initialAnalysis.getFirst().segmentId();
        Path segmentFilePath = Segment.getSegmentFilePath(volume.getConfig().dataDir(), firstSegmentId);
        assertTrue(Files.exists(segmentFilePath), "Segment file should exist before cleanup");

        // Vacuum the first segment - this moves all entries to a new segment
        VacuumContext vacuumContext = new VacuumContext(firstSegmentId, new AtomicBoolean());
        volume.vacuumSegment(vacuumContext);

        // Verify the first segment now has zero cardinality
        List<SegmentAnalysis> afterVacuumAnalysis = volume.analyze();
        assertEquals(3, afterVacuumAnalysis.size()); // 2 originals + 1 new from vacuum
        afterVacuumAnalysis.sort(Comparator.comparing(SegmentAnalysis::segmentId));
        assertEquals(0, afterVacuumAnalysis.getFirst().cardinality(), "First segment should have zero cardinality after vacuum");

        // Cleanup stale segments
        List<String> deletedFiles = volume.cleanupStaleSegments();

        // Verify the stale segment was deleted
        assertEquals(1, deletedFiles.size());
        assertTrue(deletedFiles.getFirst().contains(String.valueOf(firstSegmentId)));

        // Verify segment file is deleted from disk
        assertFalse(Files.exists(segmentFilePath), "Segment file should be deleted after cleanup");

        // Verify a segment is removed from volume analysis
        List<SegmentAnalysis> afterCleanupAnalysis = volume.analyze();
        assertEquals(2, afterCleanupAnalysis.size());
        for (SegmentAnalysis analysis : afterCleanupAnalysis) {
            assertNotEquals(firstSegmentId, analysis.segmentId(), "Stale segment should be removed");
        }
    }

    @Test
    void shouldNotCleanupLatestSegmentEvenWithZeroCardinality() throws IOException {
        // Append a small entry to create a single segment
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            volume.append(session, ByteBuffer.wrap(new byte[]{1, 2, 3}));
            tr.commit().join();
        }

        // Verify we have exactly 1 segment
        List<SegmentAnalysis> initialAnalysis = volume.analyze();
        assertEquals(1, initialAnalysis.size());

        long segmentId = initialAnalysis.getFirst().segmentId();
        Path segmentFilePath = Segment.getSegmentFilePath(volume.getConfig().dataDir(), segmentId);

        // Delete all entries to make cardinality = 0
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            volume.clearPrefix(session);
            tr.commit().join();
        }

        // Verify segment now has zero cardinality
        List<SegmentAnalysis> afterDeleteAnalysis = volume.analyze();
        assertEquals(1, afterDeleteAnalysis.size());
        assertEquals(0, afterDeleteAnalysis.getFirst().cardinality(), "Segment should have zero cardinality");

        // Cleanup stale segments - should NOT delete the latest segment
        List<String> deletedFiles = volume.cleanupStaleSegments();

        // Verify no segments were deleted
        assertTrue(deletedFiles.isEmpty(), "Latest segment should not be deleted even with zero cardinality");

        // Verify segment file still exists
        assertTrue(Files.exists(segmentFilePath), "Latest segment file should still exist");

        // Verify the segment is still in volume analysis
        List<SegmentAnalysis> afterCleanupAnalysis = volume.analyze();
        assertEquals(1, afterCleanupAnalysis.size());
        assertEquals(segmentId, afterCleanupAnalysis.getFirst().segmentId());
    }

    @Test
    void shouldReturnEmptyListWhenNoStaleSegments() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        // Fill two segments with data
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        // Verify we have 2 segments with entries
        List<SegmentAnalysis> initialAnalysis = volume.analyze();
        assertEquals(2, initialAnalysis.size());
        for (SegmentAnalysis analysis : initialAnalysis) {
            assertTrue(analysis.cardinality() > 0, "All segments should have entries");
        }

        // Cleanup stale segments - should return empty list since no stale segments
        List<String> deletedFiles = volume.cleanupStaleSegments();

        // Verify no segments were deleted
        assertTrue(deletedFiles.isEmpty(), "No segments should be deleted when none are stale");

        // Verify all segments still exist
        List<SegmentAnalysis> afterCleanupAnalysis = volume.analyze();
        assertEquals(2, afterCleanupAnalysis.size());
    }

    @Test
    void shouldCleanupMultipleStaleSegments() throws IOException, KeyNotFoundException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        // Fill two segments with data
        AppendResult result;
        ByteBuffer[] entries = new ByteBuffer[(int) numIterations];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < numIterations; i++) {
                entries[i] = randomBytes((int) bufferSize);
            }
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        // Update all entries - this moves them to new segments, leaving original segments with zero cardinality
        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        KeyEntry[] pairs = new KeyEntry[versionstampedKeys.length];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            int index = 0;
            for (Versionstamp versionstampedKey : versionstampedKeys) {
                pairs[index] = new KeyEntry(versionstampedKey, randomBytes((int) bufferSize));
                index++;
            }
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            UpdateResult updateResult = volume.update(session, pairs);
            tr.commit().join();
            updateResult.complete();
        }

        // Verify we have 4 segments (2 original + 2 new from updates)
        List<SegmentAnalysis> afterUpdateAnalysis = volume.analyze();
        assertEquals(4, afterUpdateAnalysis.size());

        // Verify first two segments have zero cardinality
        afterUpdateAnalysis.sort(Comparator.comparing(SegmentAnalysis::segmentId));
        long firstStaleSegmentId = afterUpdateAnalysis.get(0).segmentId();
        long secondStaleSegmentId = afterUpdateAnalysis.get(1).segmentId();
        assertEquals(0, afterUpdateAnalysis.get(0).cardinality());
        assertEquals(0, afterUpdateAnalysis.get(1).cardinality());

        // Get file paths for stale segments
        Path firstSegmentFilePath = Segment.getSegmentFilePath(volume.getConfig().dataDir(), firstStaleSegmentId);
        Path secondSegmentFilePath = Segment.getSegmentFilePath(volume.getConfig().dataDir(), secondStaleSegmentId);
        assertTrue(Files.exists(firstSegmentFilePath));
        assertTrue(Files.exists(secondSegmentFilePath));

        // Cleanup stale segments
        List<String> deletedFiles = volume.cleanupStaleSegments();

        // Verify both stale segments were deleted
        assertEquals(2, deletedFiles.size());

        // Verify segment files are deleted from disk
        assertFalse(Files.exists(firstSegmentFilePath), "First stale segment file should be deleted");
        assertFalse(Files.exists(secondSegmentFilePath), "Second stale segment file should be deleted");

        // Verify only 2 segments remain (the ones with data)
        List<SegmentAnalysis> afterCleanupAnalysis = volume.analyze();
        assertEquals(2, afterCleanupAnalysis.size());
        for (SegmentAnalysis analysis : afterCleanupAnalysis) {
            assertNotEquals(firstStaleSegmentId, analysis.segmentId());
            assertNotEquals(secondStaleSegmentId, analysis.segmentId());
            assertTrue(analysis.cardinality() > 0, "Remaining segments should have entries");
        }
    }
}