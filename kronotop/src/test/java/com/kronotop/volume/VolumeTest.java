/*
 * Copyright (c) 2023-2024 Kronotop
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.base.Strings;
import com.kronotop.Context;
import com.kronotop.common.utils.DirectoryLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

public class VolumeTest extends BaseVolumeTest {
    Database database;
    Context context;
    VolumeService service;
    DirectorySubspace directorySubspace;
    Volume volume;
    VolumeConfig volumeConfig;
    Random random = new Random();

    private ByteBuffer randomBytes(int size) {
        byte[] b = new byte[size];
        random.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    @BeforeEach
    public void setupVolumeTestEnvironment() throws IOException {
        database = kronotopInstance.getContext().getFoundationDB();
        context = kronotopInstance.getContext();
        service = kronotopInstance.getContext().getService(VolumeService.NAME);
        directorySubspace = getDirectorySubspace();
        volumeConfig = new VolumeConfig(directorySubspace, "append-test");
        volume = service.newVolume(volumeConfig);
    }

    @AfterEach
    public void tearDownVolumeTest() {
        volume.close();
    }

    @Test
    public void test_append() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            result = volume.append(session, entries);
            tr.commit().join();
        }
        assertEquals(2, result.getVersionstampedKeys().length);
    }

    @Test
    public void test_get() throws IOException {
        ByteBuffer[] entries = getEntries(3);

        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        List<ByteBuffer> retrievedEntries = new ArrayList<>();
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
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
    public void test_delete() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        AppendResult appendResult;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        DeleteResult deleteResult;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            deleteResult = volume.delete(session, versionstampedKeys);
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            for (Versionstamp versionstamp : versionstampedKeys) {
                assertNull(volume.get(session, versionstamp));
            }
        }

        // EntryMetadata cache
        for (Versionstamp versionstamp : versionstampedKeys) {
            assertNull(volume.get(versionstamp));
        }
    }

    @Test
    public void test_update() throws IOException, KeyNotFoundException {
        Versionstamp[] versionstampedKeys;

        {
            ByteBuffer[] entries = {
                    ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                    ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
            };
            AppendResult result;
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr);
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
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr);
                result = volume.update(session, entries);
                tr.commit().join();
            }
            result.complete();

            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr);
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
    public void test_flush() {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            assertDoesNotThrow(() -> volume.append(session, entries));
            tr.commit().join();
        }
        assertDoesNotThrow(() -> volume.flush(true));
    }

    @Test
    public void test_close() {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            assertDoesNotThrow(() -> volume.append(session, entries));
            tr.commit().join();
        }
        assertDoesNotThrow(() -> volume.close());
    }

    @Test
    public void test_reopen() throws IOException {
        Versionstamp[] versionstampedKeys;
        ByteBuffer[] entries = getEntries(2);
        {
            AppendResult result;
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr);
                result = volume.append(session, entries);
                tr.commit().join();
            }
            versionstampedKeys = result.getVersionstampedKeys();
        }

        volume.close();

        {
            Volume reopenedVolume = service.newVolume(volumeConfig);
            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr);
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
    public void test_create_new_segments() throws IOException {
        long bufferSize = 100480;
        long segmentSize = context.getConfig().getLong("volumes.segment_size");
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 1; i <= numIterations; i++) {
                Session session = new Session(tr);
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        assertEquals(2, volume.getStats().getSegments().size());
    }

    @Test
    public void test_concurrent_append_then_get_all_versionstamped_keys() throws IOException, InterruptedException {
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
                try (Transaction tr = database.createTransaction()) {
                    Session session = new Session(tr);
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

        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            for (Map.Entry<Versionstamp, ByteBuffer> entry : pairs.entrySet()) {
                ByteBuffer buffer = volume.get(session, entry.getKey());
                assertArrayEquals(entry.getValue().array(), buffer.array());
            }
        }
    }

    @Test
    public void test_getStats() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult appendResult;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        Stats stats = volume.getStats();
        assertEquals(1, stats.getSegments().size());

        for (Map.Entry<String, Stats.SegmentStats> segmentStats : stats.getSegments().entrySet()) {
            assertEquals(10, segmentStats.getValue().cardinality());
            assertTrue(segmentStats.getValue().size() > segmentStats.getValue().freeBytes());
        }

        DeleteResult deleteResult;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            deleteResult = volume.delete(session, versionstampedKeys);
            tr.commit().join();
        }
        deleteResult.complete();

        stats = volume.getStats();
        assertEquals(1, stats.getSegments().size());

        for (Map.Entry<String, Stats.SegmentStats> segmentStats : stats.getSegments().entrySet()) {
            assertEquals(0, segmentStats.getValue().cardinality());
            assertTrue(segmentStats.getValue().size() > segmentStats.getValue().freeBytes());
        }
    }

    @Test
    public void test_analyze() throws IOException {
        long bufferSize = 100480;
        long segmentSize = context.getConfig().getLong("volumes.segment_size");
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        Stats stats = volume.getStats();
        assertEquals(2, stats.getSegments().size());

        long readVersion;
        try (Transaction tr = database.createTransaction()) {
            readVersion = tr.getReadVersion().join();
        }
        List<SegmentAnalysis> analysisList = volume.analyze(readVersion);
        SegmentAnalysis analysis = analysisList.getFirst();
        Stats.SegmentStats segmentStats = stats.getSegments().get(analysis.name());

        assertNotNull(segmentStats);
        assertEquals(segmentStats.cardinality(), analysis.cardinality());
        assertEquals(segmentStats.size(), analysis.size());
        assertEquals(segmentStats.freeBytes(), analysis.size() - analysis.usedBytes());
        assertTrue(analysis.garbageRatio() > 0);
    }

    @Test
    public void test_TooManyEntriesException_before_appending() {
        ByteBuffer[] entries = getEntries(UserVersion.MAX_VALUE + 1);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            assertThrows(TooManyEntriesException.class, () -> volume.append(session, entries));
        }
    }

    @Test
    public void test_TooManyEntriesException_session() throws IOException {
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
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
    public void test_update_segment_cardinality() throws IOException, KeyNotFoundException {
        long bufferSize = 100480;
        long segmentSize = context.getConfig().getLong("volumes.segment_size");
        long numIterations = 2 * (segmentSize / bufferSize);

        AppendResult result;
        ByteBuffer[] entries = new ByteBuffer[(int) numIterations];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 1; i <= numIterations; i++) {
                entries[i - 1] = randomBytes((int) bufferSize);
            }
            Session session = new Session(tr);
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
            Session session = new Session(tr);
            UpdateResult updateResult = volume.update(session, pairs);
            tr.commit().join();
            updateResult.complete();
        }

        Stats stats = volume.getStats();
        assertEquals(4, stats.getSegments().size());
        Iterator<Map.Entry<String, Stats.SegmentStats>> iterator = stats.getSegments().entrySet().iterator();

        // Cardinality should be zero for the first two segments.
        for (int i = 0; i < 2; i++) {
            Map.Entry<String, Stats.SegmentStats> segmentStats = iterator.next();
            assertEquals(0, segmentStats.getValue().cardinality());
        }

        // All keys moved to the new segments and the first two segments will be vacuumed.
        for (int i = 2; i < 4; i++) {
            Map.Entry<String, Stats.SegmentStats> segmentStats = iterator.next();
            assertEquals(10, segmentStats.getValue().cardinality());
        }
    }

    @Test
    public void test_evictSegment() throws IOException {
        long bufferSize = 100480;
        long segmentSize = context.getConfig().getLong("volumes.segment_size");
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        long readVersion;
        try (Transaction tr = database.createTransaction()) {
            readVersion = tr.getReadVersion().join();
        }

        {
            Stats stats = volume.getStats();
            String firstSegment = stats.getSegments().keySet().iterator().next();
            volume.evictSegment(firstSegment, readVersion);
        }

        {
            Stats stats = volume.getStats();
            assertEquals(3, stats.getSegments().size());
            Iterator<Map.Entry<String, Stats.SegmentStats>> iterator = stats.getSegments().entrySet().iterator();

            // Cardinality should be zero for the first segment.
            Map.Entry<String, Stats.SegmentStats> firstSegment = iterator.next();
            assertEquals(0, firstSegment.getValue().cardinality());

            // All keys moved to the new segments.
            for (int i = 1; i < 3; i++) {
                Map.Entry<String, Stats.SegmentStats> segmentStats = iterator.next();
                assertEquals(10, segmentStats.getValue().cardinality());
            }
        }
    }

    @Test
    public void test_getRange_full_scan() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        Versionstamp[] retrievedKeys = new Versionstamp[10];
        ByteBuffer[] retrievedEntries = new ByteBuffer[10];
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            int index = 0;
            Iterable<KeyEntry> iterable = volume.getRange(session);
            for (KeyEntry keyEntry : iterable) {
                retrievedKeys[index] = keyEntry.key();
                retrievedEntries[index] = keyEntry.entry();
                index++;
            }
        }
        assertArrayEquals(versionstampedKeys, retrievedKeys);
        assertArrayEquals(entries, retrievedEntries);
    }

    @Test
    public void test_getRange_random_range() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] expectedKeys = Arrays.copyOfRange(result.getVersionstampedKeys(), 3, 7);
        ByteBuffer[] expectedEntries = new ByteBuffer[expectedKeys.length];

        for (int i = 0; i < expectedKeys.length; i++) {
            Versionstamp key = expectedKeys[i];
            ByteBuffer entry = volume.get(key);
            expectedEntries[i] = entry;
        }

        Versionstamp[] retrievedKeys = new Versionstamp[expectedKeys.length];
        ByteBuffer[] retrievedEntries = new ByteBuffer[expectedKeys.length];
        try (Transaction tr = database.createTransaction()) {
            VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(expectedKeys[0]);
            VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterThan(expectedKeys[expectedKeys.length - 1]);

            Session session = new Session(tr);
            Iterable<KeyEntry> iterable = volume.getRange(session, begin, end);
            int index = 0;
            for (KeyEntry keyEntry : iterable) {
                retrievedKeys[index] = keyEntry.key();
                retrievedEntries[index] = keyEntry.entry();
                index++;
            }
        }

        assertArrayEquals(expectedKeys, retrievedKeys);
        assertArrayEquals(expectedEntries, retrievedEntries);
    }
}