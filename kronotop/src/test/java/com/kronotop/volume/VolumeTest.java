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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.volume.handlers.PackedEntry;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
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
    void test_append() throws IOException {
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
    void test_append_IllegalArgumentException() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.append(session));
        }
    }

    @Test
    void test_get() throws IOException {
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
    void test_delete() throws IOException {
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
    void test_delete_IllegalArgumentException() throws IOException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.delete(session));
        }
    }

    @Test
    void test_update() throws IOException, KeyNotFoundException {
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
            KeyEntryPair[] entries = new KeyEntryPair[2];
            entries[0] = new KeyEntryPair(versionstampedKeys[0], ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip());
            entries[1] = new KeyEntryPair(versionstampedKeys[1], ByteBuffer.allocate(6).put("BARFOO".getBytes()).flip());
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
    void test_update_IllegalArgumentException() throws IOException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class, () -> volume.update(session));
        }
    }

    @Test
    void test_flush() {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertDoesNotThrow(() -> volume.append(session, entries));
            tr.commit().join();
        }
        assertDoesNotThrow(() -> volume.flush());
    }

    @Test
    void test_close() {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertDoesNotThrow(() -> volume.append(session, entries));
            tr.commit().join();
        }
        assertDoesNotThrow(() -> volume.close());
    }

    @Test
    void test_close_then_reopen() throws IOException {
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
    void test_reopen_write_new_entries() throws IOException {
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
    void test_create_new_segments() throws IOException {
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
    void test_concurrent_append_then_get_all_versionstamped_keys() throws IOException, InterruptedException {
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
    void test_analyze() throws IOException {
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
    void test_analyze_delete_entries() throws IOException {
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
    void test_TooManyEntriesException_before_appending() {
        ByteBuffer[] entries = getEntries(UserVersion.MAX_VALUE + 1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(TooManyEntriesException.class, () -> volume.append(session, entries));
        }
    }

    @Test
    void test_TooManyEntriesException_session() throws IOException {
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
    void test_update_segment_cardinality() throws IOException, KeyNotFoundException {
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
        KeyEntryPair[] pairs = new KeyEntryPair[versionstampedKeys.length];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            int index = 0;
            for (Versionstamp versionstampedKey : versionstampedKeys) {
                pairs[index] = new KeyEntryPair(versionstampedKey, randomBytes((int) bufferSize));
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
    void test_vacuumSegment() throws IOException {
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
            String firstSegment = segmentAnalysis.getFirst().name();
            VacuumContext vacuumContext = new VacuumContext(firstSegment, new AtomicBoolean());
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
    void test_getRange_full_scan() throws IOException {
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
            Iterable<KeyEntryPair> iterable = volume.getRange(session);
            for (KeyEntryPair keyEntry : iterable) {
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
    void test_getRange_limit() throws IOException {
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
            Iterable<KeyEntryPair> iterable = volume.getRange(session, limit);
            for (KeyEntryPair keyEntry : iterable) {
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
    void test_getRange_reverse() throws IOException {
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
            Iterable<KeyEntryPair> iterable = volume.getRange(session, true);
            for (KeyEntryPair keyEntry : iterable) {
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
    void test_getRange_random_range() throws IOException {
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
            Iterable<KeyEntryPair> iterable = volume.getRange(session, begin, end);
            int index = 0;
            for (KeyEntryPair keyEntry : iterable) {
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
    void test_prefix_isolation() throws IOException {
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
    void test_getRange_prefix_isolation() throws IOException {
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
                Iterable<KeyEntryPair> iterable = volume.getRange(session);
                for (KeyEntryPair keyEntry : iterable) {
                    index++;
                }
            }
            assertEquals(10, index);
        }

        {
            int index = 0;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, new Prefix("test"));
                Iterable<KeyEntryPair> iterable = volume.getRange(session);
                for (KeyEntryPair keyEntry : iterable) {
                    index++;
                }
            }
            assertEquals(0, index);
        }
    }

    @Test
    void test_clearPrefix() throws IOException {
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
    void test_clearPrefix_when_different_prefixes_exist() {
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
    void test_insert(@TempDir Path dataDir) throws IOException {
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
        String segmentName = segmentAnalysis.getFirst().name();

        Volume standby = service.newVolume(config);
        PackedEntry[] packedEntries = new PackedEntry[]{
                new PackedEntry(0, first),
                new PackedEntry(3, second)
        };
        standby.insert(segmentName, packedEntries);
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
    void test_insert_IllegalArgumentException() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> volume.insert("test-segment"));
    }

    @Test
    void test_getStatus_default_status() {
        assertEquals(VolumeStatus.READWRITE, volume.getStatus());
    }

    @Test
    void test_setStatus_when_getStatus() {
        volume.setStatus(VolumeStatus.READONLY);
        assertEquals(VolumeStatus.READONLY, volume.getStatus());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata.compute(tr, volume.getConfig().subspace(), (volumeMetadata -> {
                assertEquals(VolumeStatus.READONLY, volumeMetadata.getStatus());
            }));
        }
    }

    @Test
    void test_when_append_raise_VolumeReadOnlyException() throws IOException {
        volume.setStatus(VolumeStatus.READONLY);

        ByteBuffer[] entries = getEntries(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.append(session, entries));
        }
    }

    @Test
    void test_when_delete_raise_VolumeReadOnlyException() throws IOException {
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
    void test_when_update_raise_VolumeReadOnlyException() throws IOException, KeyNotFoundException {
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
        KeyEntryPair[] entries = new KeyEntryPair[2];
        entries[0] = new KeyEntryPair(versionstampedKeys[0], ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip());
        entries[1] = new KeyEntryPair(versionstampedKeys[1], ByteBuffer.allocate(6).put("BARFOO".getBytes()).flip());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            assertThrows(VolumeReadOnlyException.class, () -> volume.update(session, entries));
        }
    }

    @Test
    void test_when_clearPrefix_raise_VolumeReadOnlyException() throws IOException {
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
    void test_when_insert_raise_VolumeReadOnlyException(@TempDir Path dataDir) throws IOException {
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
        String segmentName = segmentAnalysis.getFirst().name();

        Volume standby = service.newVolume(config);
        standby.setStatus(VolumeStatus.READONLY);
        PackedEntry[] packedEntries = new PackedEntry[]{
                new PackedEntry(0, first),
                new PackedEntry(3, second)
        };
        assertThrows(VolumeReadOnlyException.class, () -> standby.insert(segmentName, packedEntries));
    }

    @Test
    void test_attributes() {
        volume.setAttribute(VolumeAttributes.SHARD_ID, 1);
        assertEquals(1, volume.getAttribute(VolumeAttributes.SHARD_ID));
    }

    @Test
    void test_first_set_attribute_then_remove() {
        volume.setAttribute(VolumeAttributes.SHARD_ID, 1);
        volume.unsetAttribute(VolumeAttributes.SHARD_ID);
        assertNull(volume.getAttribute(VolumeAttributes.SHARD_ID));
    }

    @Test
    void test_concurrent_appends_then_get_all() throws IOException, InterruptedException {
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
    void test_volume_initialization_sets_non_zero_id() {
        // Test that volume initialization correctly sets a non-zero ID in VolumeMetadata
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata metadata = VolumeMetadata.load(tr, volume.getConfig().subspace());

            // The initialize() method should have set a non-zero ID during volume construction
            assertNotEquals(0, metadata.getId(), "Volume ID should not be 0 after initialization");

            // Verify the ID is actually a valid integer (not zero)
            assertTrue(metadata.getId() != 0, "Volume ID must be set to a non-zero value");

            // Test that the ID persists across metadata loads
            VolumeMetadata reloadedMetadata = VolumeMetadata.load(tr, volume.getConfig().subspace());
            assertEquals(metadata.getId(), reloadedMetadata.getId(), "Volume ID should be consistent across loads");
        }
    }

    @Test
    void test_openSegments_prevents_unnecessary_segment_creation() throws IOException {
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
            Iterable<KeyEntryPair> iterable = reopenedVolume.getRange(session);
            for (KeyEntryPair keyEntry : iterable) {
                allRetrievedEntries.add(keyEntry.entry());
            }

            // Should have initial entries + new entries
            assertEquals(initialEntries.length + newEntries.length, allRetrievedEntries.size(),
                    "Should be able to read all entries (original + new) after reopen");
        }

        // Clean up
        reopenedVolume.close();
    }
}