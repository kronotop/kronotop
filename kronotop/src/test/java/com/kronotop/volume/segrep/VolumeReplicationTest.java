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

package com.kronotop.volume.segrep;

import com.apple.foundationdb.Transaction;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeSession;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VolumeReplicationTest extends BaseNetworkedVolumeIntegrationTest {

    private void appendEntries(int number, int length) throws IOException {
        ByteBuffer[] entries = getEntries(number, length);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }
        assertEquals(number, result.getVersionstampedKeys().length);
    }

    @Test
    void shouldReplicateFullSegmentAndVerifyChecksum(@TempDir Path destination) throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);
        // We have 1 full segment and 1 half
        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        try {
            replication.run();

            List<SegmentAnalysis> items = volume.analyze();
            assertEquals(2, items.size());


            int fullSegments = 0;
            for (SegmentAnalysis analysis : items) {
                if (analysis.size() - analysis.usedBytes() < length) {
                    fullSegments++;
                }
            }
            assertEquals(1, fullSegments);

            SegmentAnalysis analysis = items.stream().filter(i -> i.size() - i.usedBytes() < length).findFirst().orElse(null);
            assertNotNull(analysis);

            Path segmentFile = Segment.getSegmentFilePath(volume.getConfig().dataDir(), analysis.segmentId());
            byte[] expected = sha1(segmentFile.toString());
            try (var stream = Files.list(destination)) {
                stream.forEach(path -> {
                    Path replicatedSegmentFile = path.resolve(Segment.generateFileName(analysis.segmentId()));
                    byte[] actual = sha1(replicatedSegmentFile.toString());
                    assertArrayEquals(expected, actual);
                });
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicatePartialSegmentAndVerifyChecksum(@TempDir Path destination) throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);
        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        try {
            replication.run();

            List<SegmentAnalysis> items = volume.analyze();
            assertEquals(2, items.size());

            int partialSegments = 0;
            for (SegmentAnalysis analysis : items) {
                if (analysis.size() - analysis.usedBytes() >= length) {
                    partialSegments++;
                }
            }
            assertEquals(1, partialSegments);

            SegmentAnalysis analysis = items.stream().filter(i -> i.size() - i.usedBytes() >= length).findFirst().orElse(null);
            assertNotNull(analysis);

            Path segmentFile = Segment.getSegmentFilePath(volume.getConfig().dataDir(), analysis.segmentId());
            byte[] expected = sha1(segmentFile.toString());
            try (var stream = Files.list(destination)) {
                stream.forEach(path -> {
                    Path replicatedSegmentFile = path.resolve(Segment.generateFileName(analysis.segmentId()));
                    byte[] actual = sha1(replicatedSegmentFile.toString());
                    assertArrayEquals(expected, actual);
                });
            }
        } finally {
            replication.shutdown();
        }
    }
}
