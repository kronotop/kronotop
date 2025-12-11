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
import com.kronotop.internal.TransactionUtils;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class VacuumTest extends BaseVolumeIntegrationTest {

    @Test
    void shouldRemoveEmptySegmentsAfterUpdatingEntries() throws IOException {
        int bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        int numIterations = (int) (2 * (segmentSize / bufferSize));

        // Insert some data
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            ByteBuffer[] entries = new ByteBuffer[numIterations];
            for (int i = 0; i < numIterations; i++) {
                entries[i] = randomBytes(bufferSize);
            }
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KeyEntry[] pairs = new KeyEntry[versionstampedKeys.length];
            for (int i = 0; i < versionstampedKeys.length; i++) {
                Versionstamp key = versionstampedKeys[i];
                pairs[i] = new KeyEntry(key, randomBytes(bufferSize));
            }
            VolumeSession session = new VolumeSession(tr, prefix);
            UpdateResult updateResult = volume.update(session, pairs);
            tr.commit().join();
            updateResult.complete();
        } catch (KeyNotFoundException e) {
            fail("Key not found " + e.getMessage());
        }

        List<SegmentAnalysis> beforeVacuum = volume.analyze();

        // Collect empty segment's ids
        List<Long> emptySegmentIds = new ArrayList<>();
        for (SegmentAnalysis analysis : beforeVacuum) {
            if (analysis.cardinality() == 0) {
                emptySegmentIds.add(analysis.segmentId());
            }
        }
        // Verify them on the disk
        for (long segmentId : emptySegmentIds) {
            Path path = Segment.getSegmentFilePath(volume.getConfig().dataDir(), segmentId);
            boolean exists = Files.exists(path);
            assertTrue(exists);
        }

        // Some segments should have zero cardinality value.
        assertTrue(() -> {
            for (SegmentAnalysis before : beforeVacuum) {
                if (before.cardinality() == 0) {
                    return true;
                }
            }
            return false;
        });

        VacuumMetadata vacuumMetadata = new VacuumMetadata(volume.getConfig().name(), 0);
        Vacuum vacuum = new Vacuum(context, volume, vacuumMetadata);
        List<String> files = assertDoesNotThrow(vacuum::start);
        assertFalse(files.isEmpty());

        List<SegmentAnalysis> afterVacuum = volume.analyze();
        // Some segments should not have zero cardinality value.
        assertTrue(() -> {
            for (SegmentAnalysis before : afterVacuum) {
                if (before.cardinality() == 0) {
                    return false;
                }
            }
            return true;
        });

        // Empty segments must be removed
        for (long segmentId : emptySegmentIds) {
            Path path = Segment.getSegmentFilePath(volume.getConfig().dataDir(), segmentId);
            boolean exists = Files.exists(path);
            assertFalse(exists);
        }
    }

    @Test
    void shouldCompactAndReplaceSegmentsDuringVacuum() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        List<SegmentAnalysis> beforeVacuum = volume.analyze();

        // force vacuum
        VacuumMetadata vacuumMetadata = new VacuumMetadata(volume.getConfig().name(), 0);
        Vacuum vacuum = new Vacuum(context, volume, vacuumMetadata);
        List<String> files = assertDoesNotThrow(vacuum::start);
        assertFalse(files.isEmpty());

        List<SegmentAnalysis> afterVacuum = volume.analyze();
        assertTrue(() -> {
            for (SegmentAnalysis before : beforeVacuum) {
                for (SegmentAnalysis after : afterVacuum) {
                    if (before.segmentId() == after.segmentId()) {
                        return false;
                    }
                }
            }
            // Old segments deleted
            return true;
        });

        assertTrue(() -> {
            for (SegmentAnalysis before : afterVacuum) {
                if (before.cardinality() == 0) {
                    return false;
                }
            }
            return true;
        });
    }

    @Test
    void shouldConsolidateFragmentedSegmentsAfterDeletions() throws IOException {
        int bufferSize = 100;
        long segmentSize = VolumeConfiguration.segmentSize;
        int numIterations = (int) (2 * (segmentSize / bufferSize));
        int batchSize = 1000;

        // Insert some data in batches
        List<Versionstamp> allKeysList = new ArrayList<>();
        for (int offset = 0; offset < numIterations; offset += batchSize) {
            int currentBatchSize = Math.min(batchSize, numIterations - offset);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                ByteBuffer[] entries = new ByteBuffer[currentBatchSize];
                for (int i = 0; i < currentBatchSize; i++) {
                    entries[i] = randomBytes(bufferSize);
                }
                AppendResult appendResult = volume.append(session, entries);
                tr.commit().join();
                allKeysList.addAll(Arrays.asList(appendResult.getVersionstampedKeys()));
            }
        }

        Versionstamp[] keys = allKeysList.toArray(new Versionstamp[0]);
        int total = keys.length;

        int deleteCount = (int) (total * 0.60);

        List<Versionstamp> list = new ArrayList<>(Arrays.asList(keys));
        Collections.shuffle(list);
        List<Versionstamp> toDelete = list.subList(0, deleteCount);

        // Delete in batches
        for (int offset = 0; offset < toDelete.size(); offset += batchSize) {
            int currentBatchSize = Math.min(batchSize, toDelete.size() - offset);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Versionstamp[] batch = new Versionstamp[currentBatchSize];
                for (int i = 0; i < currentBatchSize; i++) {
                    batch[i] = toDelete.get(offset + i);
                }

                VolumeSession session = new VolumeSession(tr, prefix);
                DeleteResult result = volume.delete(session, batch);
                tr.commit().join();
                result.complete();
            }
        }

        List<SegmentAnalysis> beforeVacuum = volume.analyze();
        assertEquals(2, beforeVacuum.size());

        VacuumMetadata vacuumMetadata = new VacuumMetadata(volume.getConfig().name(), 0);
        Vacuum vacuum = new Vacuum(context, volume, vacuumMetadata);
        List<String> files = assertDoesNotThrow(vacuum::start);
        assertFalse(files.isEmpty());

        List<SegmentAnalysis> afterVacuum = volume.analyze();
        assertEquals(1, afterVacuum.size());

        // Some segments should not have zero cardinality value.
        assertTrue(() -> {
            for (SegmentAnalysis before : afterVacuum) {
                if (before.cardinality() == 0) {
                    return false;
                }
            }
            return true;
        });

        Set<Long> afterIds = afterVacuum.stream()
                .map(SegmentAnalysis::segmentId)
                .collect(Collectors.toSet());
        for (SegmentAnalysis sa : beforeVacuum) {
            if (afterIds.contains(sa.segmentId())) {
                fail("Segment still exists: " + sa.segmentId());
            }
        }

        long beforeUsedBytes = 0;
        int beforeCardinality = 0;
        for (SegmentAnalysis analysis : beforeVacuum) {
            beforeUsedBytes += analysis.usedBytes();
            beforeCardinality += analysis.cardinality();
        }

        long afterUsedBytes = 0;
        int afterCardinality = 0;
        for (SegmentAnalysis analysis : afterVacuum) {
            afterUsedBytes += analysis.usedBytes();
            afterCardinality += analysis.cardinality();
        }

        assertEquals(beforeUsedBytes, afterUsedBytes);
        assertEquals(beforeCardinality, afterCardinality);
    }

    @Test
    void shouldHandleConcurrentUpdatesWhileVacuuming() throws Exception {
        int bufferSize = 100;
        long segmentSize = VolumeConfiguration.segmentSize;
        int numIterations = (int) (2 * (segmentSize / bufferSize));
        int batchSize = 1000;

        // Insert initial data in batches
        List<Versionstamp> allKeysList = new ArrayList<>();
        for (int offset = 0; offset < numIterations; offset += batchSize) {
            int currentBatchSize = Math.min(batchSize, numIterations - offset);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                ByteBuffer[] entries = new ByteBuffer[currentBatchSize];
                for (int i = 0; i < currentBatchSize; i++) {
                    entries[i] = randomBytes(bufferSize);
                }
                AppendResult appendResult = volume.append(session, entries);
                tr.commit().join();
                allKeysList.addAll(Arrays.asList(appendResult.getVersionstampedKeys()));
            }
        }

        Versionstamp[] allKeys = allKeysList.toArray(new Versionstamp[0]);
        int updateCount = (int) (allKeys.length * 0.60);

        // Select 60% of keys to update
        List<Versionstamp> keyList = new ArrayList<>(Arrays.asList(allKeys));
        Collections.shuffle(keyList);
        List<Versionstamp> keysToUpdate = keyList.subList(0, updateCount);

        // Start vacuum in a separate thread with 0 garbage ratio to force it
        VacuumMetadata vacuumMetadata = new VacuumMetadata(volume.getConfig().name(), 0);
        Vacuum vacuum = new Vacuum(context, volume, vacuumMetadata);

        Thread vacuumThread = Thread.ofVirtual().start(() -> {
            assertDoesNotThrow(vacuum::start);
        });

        // Concurrently, update 60% of entries with retry logic in batches
        Thread updateThread = Thread.ofVirtual().start(() -> {
            try {
                for (int offset = 0; offset < keysToUpdate.size(); offset += batchSize) {
                    int currentBatchSize = Math.min(batchSize, keysToUpdate.size() - offset);
                    KeyEntry[] entries = new KeyEntry[currentBatchSize];
                    for (int i = 0; i < currentBatchSize; i++) {
                        entries[i] = new KeyEntry(keysToUpdate.get(offset + i), randomBytes(bufferSize));
                    }

                    // Try many times because the CI/CD hosts might be slow.
                    TransactionUtils.retry(250, Duration.ofMillis(100)).executeRunnable(() -> {
                        try (Transaction tr = context.getFoundationDB().createTransaction()) {
                            VolumeSession session = new VolumeSession(tr, prefix);
                            UpdateResult result;
                            try {
                                result = volume.update(session, entries);
                            } catch (IOException | KeyNotFoundException e) {
                                throw new RuntimeException(e);
                            }
                            tr.commit().join();
                            result.complete();
                        }
                    });
                }
            } catch (Exception e) {
                fail("Update failed: " + e);
            }
        });

        // Wait for both operations to complete
        vacuumThread.join();
        updateThread.join();

        // Verify all keys are still accessible
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            for (Versionstamp key : allKeys) {
                ByteBuffer entry = volume.get(session, key);
                assertNotNull(entry, "Entry should still be accessible after concurrent vacuum and update");
            }
        }

        // Verify no segments have zero cardinality
        List<SegmentAnalysis> afterVacuum = volume.analyze();
        assertTrue(() -> {
            for (SegmentAnalysis analysis : afterVacuum) {
                if (analysis.cardinality() == 0) {
                    return false;
                }
            }
            return true;
        });
    }

    @Test
    void shouldHandleConcurrentDeletionsWhileVacuuming() throws Exception {
        int bufferSize = 100;
        long segmentSize = VolumeConfiguration.segmentSize;
        int numIterations = (int) (2 * (segmentSize / bufferSize));
        int batchSize = 1000;

        // Insert initial data in batches
        List<Versionstamp> allKeysList = new ArrayList<>();
        for (int offset = 0; offset < numIterations; offset += batchSize) {
            int currentBatchSize = Math.min(batchSize, numIterations - offset);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                ByteBuffer[] entries = new ByteBuffer[currentBatchSize];
                for (int i = 0; i < currentBatchSize; i++) {
                    entries[i] = randomBytes(bufferSize);
                }
                AppendResult appendResult = volume.append(session, entries);
                tr.commit().join();
                allKeysList.addAll(Arrays.asList(appendResult.getVersionstampedKeys()));
            }
        }

        Versionstamp[] allKeys = allKeysList.toArray(new Versionstamp[0]);
        int deleteCount = (int) (allKeys.length * 0.60);

        // Select 60% of keys to delete
        List<Versionstamp> keyList = new ArrayList<>(Arrays.asList(allKeys));
        Collections.shuffle(keyList);
        List<Versionstamp> keysToDelete = keyList.subList(0, deleteCount);
        List<Versionstamp> survivingKeys = keyList.subList(deleteCount, allKeys.length);

        // Start vacuum in a separate thread with 0 garbage ratio to force it
        VacuumMetadata vacuumMetadata = new VacuumMetadata(volume.getConfig().name(), 0);
        Vacuum vacuum = new Vacuum(context, volume, vacuumMetadata);

        Thread vacuumThread = Thread.ofVirtual().start(() -> {
            assertDoesNotThrow(vacuum::start);
        });

        // Concurrently, delete 60% of entries with retry logic in batches
        Thread deleteThread = Thread.ofVirtual().start(() -> {
            try {
                for (int offset = 0; offset < keysToDelete.size(); offset += batchSize) {
                    int currentBatchSize = Math.min(batchSize, keysToDelete.size() - offset);
                    Versionstamp[] keysArray = new Versionstamp[currentBatchSize];
                    for (int i = 0; i < currentBatchSize; i++) {
                        keysArray[i] = keysToDelete.get(offset + i);
                    }

                    // Try many times because the CI/CD hosts might be slow.
                    TransactionUtils.retry(250, Duration.ofMillis(100)).executeRunnable(() -> {
                        try (Transaction tr = context.getFoundationDB().createTransaction()) {
                            VolumeSession session = new VolumeSession(tr, prefix);
                            DeleteResult result = volume.delete(session, keysArray);
                            tr.commit().join();
                            result.complete();
                        }
                    });
                }
            } catch (Exception e) {
                fail("Delete failed: " + e);
            }
        });

        // Wait for both operations to complete
        vacuumThread.join();
        deleteThread.join();

        // Verify surviving keys are still accessible
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            for (Versionstamp key : survivingKeys) {
                ByteBuffer entry = volume.get(session, key);
                assertNotNull(entry, "Surviving entry should still be accessible after concurrent vacuum and delete");
            }
        }

        // Verify deleted keys are not accessible
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            for (Versionstamp key : keysToDelete) {
                assertNull(volume.get(session, key), "Deleted entry should not be accessible");
            }
        }

        // Verify no segments have zero cardinality
        List<SegmentAnalysis> afterVacuum = volume.analyze();
        assertTrue(() -> {
            for (SegmentAnalysis analysis : afterVacuum) {
                if (analysis.cardinality() == 0) {
                    return false;
                }
            }
            return true;
        });
    }
}