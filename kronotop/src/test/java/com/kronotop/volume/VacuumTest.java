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
import com.kronotop.volume.segment.SegmentAnalysis;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
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

        // Some segments should have zero cardinality value.
        assertTrue(() -> {
            for (SegmentAnalysis before : volume.analyze()) {
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

        Versionstamp[] keys = appendResult.getVersionstampedKeys();
        int total = keys.length;

        int deleteCount = (int) (total * 0.60);

        List<Versionstamp> list = new ArrayList<>(Arrays.asList(keys));
        Collections.shuffle(list);
        List<Versionstamp> toDelete = list.subList(0, deleteCount);

        int batchSize = 100;

        for (int offset = 0; offset < toDelete.size(); offset += batchSize) {
            int end = Math.min(offset + batchSize, toDelete.size());
            try (Transaction tr = context.getFoundationDB().createTransaction()) {

                int len = end - offset;
                Versionstamp[] batch = new Versionstamp[len];

                for (int i = 0; i < len; i++) {
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
}