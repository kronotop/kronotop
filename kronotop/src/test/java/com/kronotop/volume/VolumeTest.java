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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.replication.Host;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

public class VolumeTest extends BaseVolumeIntegrationTest {
    Random random = new Random();

    private ByteBuffer randomBytes(int size) {
        byte[] b = new byte[size];
        random.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    @Test
    public void test_append() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
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
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        List<ByteBuffer> retrievedEntries = new ArrayList<>();
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
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
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        DeleteResult deleteResult;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            deleteResult = volume.delete(session, versionstampedKeys);
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            for (Versionstamp versionstamp : versionstampedKeys) {
                assertNull(volume.get(session, versionstamp));
            }
        }

        Session session = new Session(prefix);
        // EntryMetadata cache
        for (Versionstamp versionstamp : versionstampedKeys) {
            assertNull(volume.get(session, versionstamp));
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
                Session session = new Session(tr, redisVolumeSyncerPrefix);
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
                Session session = new Session(tr, redisVolumeSyncerPrefix);
                result = volume.update(session, entries);
                tr.commit().join();
            }
            result.complete();

            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, redisVolumeSyncerPrefix);
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
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            assertDoesNotThrow(() -> volume.append(session, entries));
            tr.commit().join();
        }
        assertDoesNotThrow(() -> volume.flush(true));
    }

    @Test
    public void test_close() {
        ByteBuffer[] entries = getEntries(2);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
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
                Session session = new Session(tr, redisVolumeSyncerPrefix);
                result = volume.append(session, entries);
                tr.commit().join();
            }
            versionstampedKeys = result.getVersionstampedKeys();
        }

        volume.close();

        {
            Volume reopenedVolume = service.newVolume(volume.getConfig());
            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, redisVolumeSyncerPrefix);
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
    public void test_reopen_write_new_entries() throws IOException {
        List<Versionstamp> versionstampedKeys;

        ByteBuffer[] firstEntries = getEntries(2);
        {
            AppendResult result;
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, redisVolumeSyncerPrefix);
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
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, redisVolumeSyncerPrefix);
                result = reopenedVolume.append(session, secondEntries);
                tr.commit().join();
            }
            versionstampedKeys.addAll(Arrays.stream(result.getVersionstampedKeys()).toList());

            List<ByteBuffer> retrievedEntries = new ArrayList<>();
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, redisVolumeSyncerPrefix);
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
    public void test_create_new_segments() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 1; i <= numIterations; i++) {
                Session session = new Session(tr, redisVolumeSyncerPrefix);
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        assertEquals(2, volume.analyze().size());
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
                    Session session = new Session(tr, redisVolumeSyncerPrefix);
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
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            for (Map.Entry<Versionstamp, ByteBuffer> entry : pairs.entrySet()) {
                ByteBuffer buffer = volume.get(session, entry.getKey());
                assertArrayEquals(entry.getValue().array(), buffer.array());
            }
        }
    }

    @Test
    public void test_analyze() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        try (Transaction tr = database.createTransaction()) {
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
    public void test_analyze_delete_entries() throws IOException {
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            appendResult = volume.append(session, getEntries(10));
            tr.commit().join();
        }

        DeleteResult deleteResult;
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();
        Versionstamp[] keys = Arrays.copyOfRange(versionstampedKeys, 3, 7);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            deleteResult = volume.delete(session, keys);
            tr.commit().join();
        }
        deleteResult.complete();

        try (Transaction tr = database.createTransaction()) {
            List<SegmentAnalysis> analysis = volume.analyze(tr);
            SegmentAnalysis segmentAnalysis = analysis.getFirst();
            assertTrue(segmentAnalysis.garbageRatio() > 0);
            long garbageBytes = segmentAnalysis.size() - segmentAnalysis.freeBytes() - segmentAnalysis.usedBytes();
            assertEquals(40, garbageBytes); // We deleted 4 items and the test entry size is 10.
        }
    }

    @Test
    public void test_TooManyEntriesException_before_appending() {
        ByteBuffer[] entries = getEntries(UserVersion.MAX_VALUE + 1);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            assertThrows(TooManyEntriesException.class, () -> volume.append(session, entries));
        }
    }

    @Test
    public void test_TooManyEntriesException_session() throws IOException {
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
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
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        AppendResult result;
        ByteBuffer[] entries = new ByteBuffer[(int) numIterations];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 1; i <= numIterations; i++) {
                entries[i - 1] = randomBytes((int) bufferSize);
            }
            Session session = new Session(tr, redisVolumeSyncerPrefix);
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
            Session session = new Session(tr, redisVolumeSyncerPrefix);
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
    public void test_vacuumSegment() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
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
            List<SegmentAnalysis> segmentAnalysis = volume.analyze();
            String firstSegment = segmentAnalysis.getFirst().name();
            volume.vacuumSegment(firstSegment, readVersion);
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
    public void test_getRange_full_scan() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        Versionstamp[] retrievedKeys = new Versionstamp[10];
        ByteBuffer[] retrievedEntries = new ByteBuffer[10];
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            int index = 0;
            Iterable<KeyEntry> iterable = volume.getRange(session);
            for (KeyEntry keyEntry : iterable) {
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
    public void test_getRange_random_range() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] expectedKeys = Arrays.copyOfRange(result.getVersionstampedKeys(), 3, 7);
        ByteBuffer[] expectedEntries = new ByteBuffer[expectedKeys.length];

        Session sessionWithoutTransaction = new Session(redisVolumeSyncerPrefix);
        for (int i = 0; i < expectedKeys.length; i++) {
            Versionstamp key = expectedKeys[i];
            ByteBuffer entry = volume.get(sessionWithoutTransaction, key);
            expectedEntries[i] = entry;
        }

        Versionstamp[] retrievedKeys = new Versionstamp[expectedKeys.length];
        ByteBuffer[] retrievedEntries = new ByteBuffer[expectedKeys.length];
        try (Transaction tr = database.createTransaction()) {
            VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(expectedKeys[0]);
            VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterThan(expectedKeys[expectedKeys.length - 1]);

            Session session = new Session(tr, redisVolumeSyncerPrefix);
            Iterable<KeyEntry> iterable = volume.getRange(session, begin, end);
            int index = 0;
            for (KeyEntry keyEntry : iterable) {
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
    public void test_VolumeMetadata_compute() throws UnknownHostException {
        Host host = new Host(Role.STANDBY, createMemberWithEphemeralPort());
        try (Transaction tr = database.createTransaction()) {
            VolumeMetadata.compute(tr, volume.getConfig().subspace(), (volumeMetadata) -> {
                volumeMetadata.setStandby(host);
            });
            tr.commit().join();
        }

        try (Transaction tr = database.createTransaction()) {
            VolumeMetadata.compute(tr, volume.getConfig().subspace(), (volumeMetadata) -> {
                assertEquals(1, volumeMetadata.getStandbyHosts().size());
                assertEquals(host, volumeMetadata.getStandbyHosts().getFirst());
            });
        }
    }

    @Test
    public void test_prefix_isolation() throws IOException {
        Prefix prefixOne = new Prefix("one");
        Prefix prefixTwo = new Prefix("two");
        String dataOne = "prefix-one-entry";
        String dataTwo = "prefix-two-entry";

        Versionstamp keyOne;
        Versionstamp keyTwo;

        {
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, prefixOne);
                ByteBuffer entry = ByteBuffer.allocate(dataOne.length()).put(dataOne.getBytes()).flip();
                AppendResult result = volume.append(session, entry);
                tr.commit().join();
                keyOne = result.getVersionstampedKeys()[0];
            }
        }

        {
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, prefixTwo);
                ByteBuffer entry = ByteBuffer.allocate(dataTwo.length()).put(dataTwo.getBytes()).flip();
                AppendResult result = volume.append(session, entry);
                tr.commit().join();
                keyTwo = result.getVersionstampedKeys()[0];
            }
        }

        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, prefixOne);
            ByteBuffer entry = volume.get(session, keyTwo);
            assertNull(entry);

            entry = volume.get(session, keyOne);
            assertNotNull(entry);
            assertEquals(dataOne, new String(entry.array()));
        }

        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, prefixTwo);
            ByteBuffer entry = volume.get(session, keyOne);
            assertNull(entry);

            entry = volume.get(session, keyTwo);
            assertNotNull(entry);
            assertEquals(dataTwo, new String(entry.array()));
        }

        {
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, prefixOne);
                DeleteResult result = volume.delete(session, keyTwo);
                tr.commit().join();
                result.complete();
            }

            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, prefixTwo);
                ByteBuffer entry = volume.get(session, keyTwo);
                assertNotNull(entry);
                assertEquals(dataTwo, new String(entry.array()));
            }
        }
    }

    @Test
    public void test_getRange_prefix_isolation() throws IOException {
        ByteBuffer[] entries = getEntries(10);
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        {
            int index = 0;
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, redisVolumeSyncerPrefix);
                Iterable<KeyEntry> iterable = volume.getRange(session);
                for (KeyEntry keyEntry : iterable) {
                    index++;
                }
            }
            assertEquals(10, index);
        }

        {
            int index = 0;
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, new Prefix("test"));
                Iterable<KeyEntry> iterable = volume.getRange(session);
                for (KeyEntry keyEntry : iterable) {
                    index++;
                }
            }
            assertEquals(0, index);
        }
    }
}