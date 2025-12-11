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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.cluster.client.protocol.VolumeInspectCursorResponse;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.volume.*;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentAnalysis;
import io.lettuce.core.ClientOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VolumeReplicationTest extends BaseNetworkedVolumeIntegrationTest {

    private void replicationStopTrigger(VolumeReplication replication, DirectorySubspace standbySubspace) {
        Thread.ofVirtual().start(() -> {
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    if (ReplicationState.readStage(tr, standbySubspace, 1L) == Stage.CHANGE_DATA_CAPTURE) {
                        replication.shutdown();
                        return true;
                    }
                    return false;
                }
            });
        });
    }

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
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();
        try {
            replicationStopTrigger(replication, standbySubspace);
            replication.start();

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

            ReplicationStatus status = TransactionUtils.execute(context, tr ->
                    ReplicationState.readStatus(tr, standbySubspace, analysis.segmentId())
            );
            assertEquals(ReplicationStatus.DONE, status);
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
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();
        try {
            replicationStopTrigger(replication, standbySubspace);
            replication.start();

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
            ReplicationStatus status = TransactionUtils.execute(context, tr ->
                    ReplicationState.readStatus(tr, standbySubspace, analysis.segmentId())
            );
            assertEquals(ReplicationStatus.RUNNING, status);

            Stage stage = TransactionUtils.execute(context, tr ->
                    ReplicationState.readStage(tr, standbySubspace, analysis.segmentId())
            );
            assertEquals(Stage.CHANGE_DATA_CAPTURE, stage);
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldSetTailPointerDuringReplication(@TempDir Path destination) throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();
        try {
            replicationStopTrigger(replication, standbySubspace);
            replication.start();

            List<SegmentAnalysis> items = volume.analyze();
            assertEquals(2, items.size());

            // Find the active (latest) segment
            long activeSegmentId = items.stream()
                    .mapToLong(SegmentAnalysis::segmentId)
                    .max()
                    .orElseThrow();

            for (SegmentAnalysis analysis : items) {
                List<Long> tailPointer = TransactionUtils.execute(context, tr ->
                        ReplicationState.readTailPointer(tr, standbySubspace, analysis.segmentId())
                );
                long sequenceNumber = tailPointer.get(0);
                long nextPosition = tailPointer.get(1);

                if (analysis.segmentId() == activeSegmentId) {
                    assertTrue(sequenceNumber > 0,
                            "Active segment should have positive sequence number");
                } else {
                    assertEquals(-1, sequenceNumber,
                            "Non-active segment should have sequence number -1");
                }
                assertTrue(nextPosition > 0, "Next position should be set for segment " + analysis.segmentId());
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateAndConvergeSegmentsToExactMatch(@TempDir Path destination) throws Exception {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        VolumeReplication replication =
                new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());

        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        // CDC trigger: write extra entries when CHANGE_DATA_CAPTURE stage is reached
        Thread.ofVirtual().start(() -> {
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, 1L) == Stage.CHANGE_DATA_CAPTURE;
                }
            });

            for (int i = 0; i < 10; i++) {
                try {
                    appendEntries(1, 10);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            // Analyze original volume: expecting 2 segments
            await().atMost(Duration.ofSeconds(15)).until(() -> volume.analyze().size() == 2);

            List<SegmentAnalysis> items = volume.analyze();
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();

                Path originalSegmentFile = Segment.getSegmentFilePath(
                        volume.getConfig().dataDir(), segmentId
                );

                // Goal: verify that the SHA1 of the replicated segment file under destination
                // matches the SHA1 of the original segment file within a 15-second window.
                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    // SHA1 of the original segment file at this moment
                    byte[] expected = sha1(originalSegmentFile.toString());

                    // Iterate through all volume directories/paths under destination
                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile =
                                    volumeDir.resolve(Segment.generateFileName(segmentId));

                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }

                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldResumeReplicationAfterRestart(@TempDir Path destination) throws Exception {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        // Identify the full segment (will be marked DONE first)
        List<SegmentAnalysis> items = volume.analyze();
        assertEquals(2, items.size());
        SegmentAnalysis fullSegment = items.stream()
                .filter(i -> i.size() - i.usedBytes() < length)
                .findFirst()
                .orElseThrow();
        long fullSegmentId = fullSegment.segmentId();

        // First replication run - stop after first segment is replicated
        VolumeReplication replication1 = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication1.createOrOpenStandbySubspace();

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        // Stop replication after the full segment reaches DONE status
        Thread.ofVirtual().start(() -> {
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStatus(tr, standbySubspace, fullSegmentId) == ReplicationStatus.DONE;
                }
            });
            replication1.shutdown();
            shutdownLatch.countDown();
        });

        Thread.ofVirtual().start(replication1::start);

        shutdownLatch.await();

        // Capture position after first run
        long positionAfterFirstRun = TransactionUtils.execute(context, tr ->
                ReplicationState.readPosition(tr, standbySubspace, fullSegmentId)
        );
        assertTrue(positionAfterFirstRun > 0, "Position should be set after first run");

        // Second replication run - should resume from where it left off
        VolumeReplication replication2 = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());

        // Insert additional entries concurrently during the second replication run
        CountDownLatch writesCompletedLatch = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    appendEntries(1, 10);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            writesCompletedLatch.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication2::start);

        try {
            // Wait for writes to complete before verification
            assertTrue(writesCompletedLatch.await(15, TimeUnit.SECONDS));

            // Re-analyze volume after writes complete to get current state
            List<SegmentAnalysis> currentItems = volume.analyze();

            // Verify segments converge to exact match
            for (SegmentAnalysis analysis : currentItems) {
                long segmentId = analysis.segmentId();
                Path originalSegmentFile = Segment.getSegmentFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());

                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(Segment.generateFileName(segmentId));

                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }

                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }
        } finally {
            replication2.shutdown();
        }
    }

    @Test
    void shouldHandleEmptyVolumeReplication(@TempDir Path destination) throws Exception {
        // Start with an empty volume - no entries appended yet
        List<SegmentAnalysis> initialItems = volume.analyze();
        assertTrue(initialItems.isEmpty(), "Volume should be empty initially");

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());

        // Write entries concurrently after replication starts
        CountDownLatch writesCompletedLatch = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            // Wait briefly for replication to start
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Now write some entries
            for (int i = 0; i < 10; i++) {
                try {
                    appendEntries(10, 100);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            writesCompletedLatch.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            // Wait for writes to complete
            assertTrue(writesCompletedLatch.await(15, TimeUnit.SECONDS));

            // Volume should now have segments
            List<SegmentAnalysis> currentItems = volume.analyze();
            assertFalse(currentItems.isEmpty(), "Volume should have segments after writes");

            // Verify segments converge to exact match
            for (SegmentAnalysis analysis : currentItems) {
                long segmentId = analysis.segmentId();
                Path originalSegmentFile = Segment.getSegmentFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());

                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(Segment.generateFileName(segmentId));

                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }

                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldTransitionFromSegmentReplicationToCDC(@TempDir Path destination) throws Exception {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        List<SegmentAnalysis> items = volume.analyze();
        assertEquals(2, items.size());

        // Identify the full segment (will go through SEGMENT_REPLICATION -> DONE)
        SegmentAnalysis fullSegment = items.stream()
                .filter(i -> i.size() - i.usedBytes() < length)
                .findFirst()
                .orElseThrow();
        long fullSegmentId = fullSegment.segmentId();

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        // Track stage transitions for the full segment
        CountDownLatch segmentReplicationSeen = new CountDownLatch(1);
        CountDownLatch segmentDone = new CountDownLatch(1);
        CountDownLatch cdcSeen = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            // Wait for SEGMENT_REPLICATION stage on first segment
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, fullSegmentId) == Stage.SEGMENT_REPLICATION;
                }
            });
            segmentReplicationSeen.countDown();

            // Wait for first segment to be DONE
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStatus(tr, standbySubspace, fullSegmentId) == ReplicationStatus.DONE;
                }
            });
            segmentDone.countDown();

            // Wait for CDC stage (on any segment - the last active segment will be in CDC)
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    for (SegmentAnalysis analysis : items) {
                        if (ReplicationState.readStage(tr, standbySubspace, analysis.segmentId()) == Stage.CHANGE_DATA_CAPTURE) {
                            return true;
                        }
                    }
                    return false;
                }
            });
            cdcSeen.countDown();

            // Shutdown after CDC is reached
            replication.shutdown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            // Verify SEGMENT_REPLICATION stage was observed
            assertTrue(segmentReplicationSeen.await(15, TimeUnit.SECONDS),
                    "SEGMENT_REPLICATION stage should be observed");

            // Verify segment completed replication
            assertTrue(segmentDone.await(15, TimeUnit.SECONDS),
                    "First segment should complete with DONE status");

            // Verify transition to CHANGE_DATA_CAPTURE stage
            assertTrue(cdcSeen.await(15, TimeUnit.SECONDS),
                    "CHANGE_DATA_CAPTURE stage should be reached after SEGMENT_REPLICATION");

            // Verify first segment final state
            Stage firstSegmentStage = TransactionUtils.execute(context, tr ->
                    ReplicationState.readStage(tr, standbySubspace, fullSegmentId)
            );
            assertEquals(Stage.SEGMENT_REPLICATION, firstSegmentStage);

            ReplicationStatus firstSegmentStatus = TransactionUtils.execute(context, tr ->
                    ReplicationState.readStatus(tr, standbySubspace, fullSegmentId)
            );
            assertEquals(ReplicationStatus.DONE, firstSegmentStatus);
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldHandleSegmentCreationDuringCDC(@TempDir Path destination) throws Exception {
        // Start with a partial segment to quickly enter CDC mode
        int length = 1024;
        int number = 10;
        appendEntries(number, length);

        List<SegmentAnalysis> initialItems = volume.analyze();
        assertEquals(1, initialItems.size(), "Should start with 1 partial segment");
        long initialSegmentId = initialItems.getFirst().segmentId();

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        // Write enough data during CDC to create new segments
        int entriesPerSegment = Math.toIntExact(volume.getConfig().segmentSize() / length);
        CountDownLatch writesCompletedLatch = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            // Wait for CDC stage on initial segment
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, initialSegmentId) == Stage.CHANGE_DATA_CAPTURE;
                }
            });

            // Write enough data to create 2 new segments
            try {
                appendEntries(entriesPerSegment * 2, length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            writesCompletedLatch.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            // Wait for writes to complete
            assertTrue(writesCompletedLatch.await(15, TimeUnit.SECONDS));

            // Volume should now have more segments
            List<SegmentAnalysis> currentItems = volume.analyze();
            assertTrue(currentItems.size() >= 3, "Should have at least 3 segments after writes");

            // Verify all segments converge to exact match
            for (SegmentAnalysis analysis : currentItems) {
                long segmentId = analysis.segmentId();
                Path originalSegmentFile = Segment.getSegmentFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());

                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(Segment.generateFileName(segmentId));

                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }

                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateMultipleSegmentsSequentially(@TempDir Path destination) throws Exception {
        // Create 3 full segments + 1 partial (to have clear DONE transitions)
        int length = 1024;
        int entriesPerSegment = Math.toIntExact(volume.getConfig().segmentSize() / length);
        appendEntries(entriesPerSegment * 3 + 10, length);

        List<SegmentAnalysis> items = volume.analyze();
        assertEquals(4, items.size(), "Should have 4 segments (3 full + 1 partial)");

        // Identify full segments (will get DONE status)
        List<Long> fullSegmentIds = items.stream()
                .filter(i -> i.size() - i.usedBytes() < length)
                .map(SegmentAnalysis::segmentId)
                .sorted()
                .toList();
        assertEquals(3, fullSegmentIds.size(), "Should have 3 full segments");

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        // Track sequential completion of full segments
        CountDownLatch allFullSegmentsDone = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            // Wait for each full segment to complete in order
            for (Long segmentId : fullSegmentIds) {
                await().atMost(Duration.ofSeconds(30)).until(() -> {
                    try (Transaction tr = context.getFoundationDB().createTransaction()) {
                        return ReplicationState.readStatus(tr, standbySubspace, segmentId) == ReplicationStatus.DONE;
                    }
                });
            }
            allFullSegmentsDone.countDown();

            // Wait for CDC to start on the partial segment, then shutdown
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    for (SegmentAnalysis analysis : items) {
                        if (ReplicationState.readStage(tr, standbySubspace, analysis.segmentId()) == Stage.CHANGE_DATA_CAPTURE) {
                            return true;
                        }
                    }
                    return false;
                }
            });
            replication.shutdown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            // Wait for all full segments to complete
            assertTrue(allFullSegmentsDone.await(60, TimeUnit.SECONDS),
                    "All full segments should complete replication");

            // Verify all full segments have DONE status and correct checksums
            for (Long segmentId : fullSegmentIds) {
                // Verify status is DONE
                ReplicationStatus status = TransactionUtils.execute(context, tr ->
                        ReplicationState.readStatus(tr, standbySubspace, segmentId)
                );
                assertEquals(ReplicationStatus.DONE, status,
                        "Segment " + segmentId + " should have DONE status");

                // Verify stage is SEGMENT_REPLICATION (full segments stay in this stage)
                Stage stage = TransactionUtils.execute(context, tr ->
                        ReplicationState.readStage(tr, standbySubspace, segmentId)
                );
                assertEquals(Stage.SEGMENT_REPLICATION, stage,
                        "Segment " + segmentId + " should have SEGMENT_REPLICATION stage");

                // Verify checksum match
                Path originalSegmentFile = Segment.getSegmentFilePath(volume.getConfig().dataDir(), segmentId);
                byte[] expected = sha1(originalSegmentFile.toString());

                try (var stream = Files.list(destination)) {
                    boolean matched = stream.anyMatch(volumeDir -> {
                        Path replicatedSegmentFile = volumeDir.resolve(Segment.generateFileName(segmentId));
                        if (!Files.exists(replicatedSegmentFile)) {
                            return false;
                        }
                        byte[] actual = sha1(replicatedSegmentFile.toString());
                        return Arrays.equals(expected, actual);
                    });
                    assertTrue(matched, "Segment " + segmentId + " checksum should match");
                }
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldHandleReplicationWithDeleteOperations(@TempDir Path destination) throws Exception {
        ReplicationClient replicationClient = new ReplicationClient(context, SHARD_KIND, SHARD_ID);
        replicationClient.connect(ClientOptions.create());

        int length = 1024;
        int number = 100;
        ByteBuffer[] entries = getEntries(number, length);

        // Append entries and capture versionstamps for later deletion
        Versionstamp[] versionstampedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = appendResult.getVersionstampedKeys();
        }
        assertEquals(number, versionstampedKeys.length);

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());

        CountDownLatch deletesCompletedLatch = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            // Delete half of the entries
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                Versionstamp[] keysToDelete = new Versionstamp[number / 2];
                System.arraycopy(versionstampedKeys, 0, keysToDelete, 0, number / 2);
                volume.delete(session, keysToDelete);
                tr.commit().join();
            }
            deletesCompletedLatch.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            // Wait for deletes to complete
            assertTrue(deletesCompletedLatch.await(15, TimeUnit.SECONDS));

            await().atMost(Duration.ofSeconds(15)).until(() -> {
                VolumeInspectCursorResponse response = replicationClient.conn().sync().volumeInspectCursor(volumeConfig.name());
                String latestVersionstamp = VersionstampUtil.base32HexEncode(versionstampedKeys[versionstampedKeys.length - 1]);
                return response.versionstamp().equals(latestVersionstamp);
            });

            // Create a new Volume instance with the replicated data
            Path replicatedDataDir = Files.list(destination).findFirst().orElseThrow();
            VolumeConfig replicatedVolumeConfig = new VolumeConfig(
                    volumeConfig.subspace(),
                    volumeConfig.name(),
                    replicatedDataDir.toString(),
                    volumeConfig.segmentSize()
            );
            Volume replicatedVolume = new Volume(context, replicatedVolumeConfig);

            // Verify deleted keys return null (metadata is shared via FoundationDB)
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 0; i < number / 2; i++) {
                    ByteBuffer result = replicatedVolume.get(session, versionstampedKeys[i]);
                    assertNull(result, "Deleted key at index " + i + " should return null");
                }
            }

            // Verify surviving keys return valid data from replicated segments
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = number / 2; i < number; i++) {
                    ByteBuffer result = replicatedVolume.get(session, versionstampedKeys[i]);
                    assertNotNull(result, "Surviving key at index " + i + " should return data");
                }
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateAndConvergeSegmentsUnderHeavyConcurrentWrites(@TempDir Path destination) throws Exception {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        VolumeReplication replication =
                new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());

        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        // CDC trigger: write extra entries when CHANGE_DATA_CAPTURE stage is reached
        int finalNumber = number;
        Thread.ofVirtual().start(() -> {
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, 1L) == Stage.CHANGE_DATA_CAPTURE;
                }
            });

            for (int i = 0; i < 10; i++) {
                try {
                    appendEntries(finalNumber, length);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // Replication thread, works at background
        Thread.ofVirtual().start(replication::start);

        try {
            // Analyze original volume: expecting 17 segments
            await().atMost(Duration.ofSeconds(15)).until(() -> volume.analyze().size() == 17);

            List<SegmentAnalysis> items = volume.analyze();
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();

                Path originalSegmentFile = Segment.getSegmentFilePath(
                        volume.getConfig().dataDir(), segmentId
                );

                // Goal: verify that the SHA1 of the replicated segment file under destination
                // matches the SHA1 of the original segment file within a 15-second window.
                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    // SHA1 of the original segment file at this moment
                    byte[] expected = sha1(originalSegmentFile.toString());

                    // Iterate through all volume directories/paths under destination
                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile =
                                    volumeDir.resolve(Segment.generateFileName(segmentId));

                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }

                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldVerifyStandbyCaughtUpWithPrimary(@TempDir Path destination) throws Exception {
        ReplicationClient replicationClient = new ReplicationClient(context, SHARD_KIND, SHARD_ID);
        replicationClient.connect(ClientOptions.create());

        int length = 1024;
        int number = 100;
        appendEntries(number, length);

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        // Replication thread - runs at background and waits for new events in CDC mode
        Thread.ofVirtual().start(replication::start);

        try {
            // Wait until the standby has caught up with the primary
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                VolumeInspectCursorResponse primaryCursor = replicationClient.conn().sync().volumeInspectCursor(volumeConfig.name());
                if (!primaryCursor.hasData()) {
                    return false;
                }

                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    // Check if the active segment is in CDC mode
                    Stage stage = ReplicationState.readStage(tr, standbySubspace, primaryCursor.activeSegmentId());
                    if (stage != Stage.CHANGE_DATA_CAPTURE) {
                        return false;
                    }

                    // Compare tail pointer from standby with primary's cursor
                    List<Long> tailPointer = ReplicationState.readTailPointer(tr, standbySubspace, primaryCursor.activeSegmentId());
                    if (tailPointer.isEmpty()) {
                        return false;
                    }

                    long standbySequenceNumber = tailPointer.get(0);
                    long standbyNextPosition = tailPointer.get(1);

                    // Verify sequence number and position match
                    return standbySequenceNumber == primaryCursor.sequenceNumber()
                            && standbyNextPosition == primaryCursor.nextPosition();
                }
            });

            // Final verification
            VolumeInspectCursorResponse primaryCursor = replicationClient.conn().sync().volumeInspectCursor(volumeConfig.name());
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                List<Long> tailPointer = ReplicationState.readTailPointer(tr, standbySubspace, primaryCursor.activeSegmentId());
                assertEquals(primaryCursor.sequenceNumber(), tailPointer.get(0), "Sequence number should match");
                assertEquals(primaryCursor.nextPosition(), tailPointer.get(1), "Next position should match");

                Stage stage = ReplicationState.readStage(tr, standbySubspace, primaryCursor.activeSegmentId());
                assertEquals(Stage.CHANGE_DATA_CAPTURE, stage, "Active segment should be in CDC mode");
            }
        } finally {
            replicationClient.shutdown();
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateNewSegmentsCreatedDuringSegmentReplication(@TempDir Path destination) throws Exception {
        // Start with segment 0 as active (full segment so SR can complete with DONE)
        int length = 1024;
        int entriesPerSegment = Math.toIntExact(volume.getConfig().segmentSize() / length);

        // Fill segment 0 completely + a few entries in segment 1
        appendEntries(entriesPerSegment + 10, length);

        List<SegmentAnalysis> initialItems = volume.analyze();
        assertEquals(2, initialItems.size(), "Should start with segment 0 (full) and segment 1 (partial)");

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        CountDownLatch writesCompleted = new CountDownLatch(1);

        // Writer thread: when segment 0 SR starts, create many new segments concurrently
        Thread.ofVirtual().start(() -> {
            // Wait for segment 0 to enter SEGMENT_REPLICATION stage
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, 0L) == Stage.SEGMENT_REPLICATION;
                }
            });

            // Now create segments 2, 3, 4, 5 by writing enough data while segment 0 SR is running
            try {
                appendEntries(entriesPerSegment * 4, length);
                //Thread.sleep(100);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            writesCompleted.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            // Wait for concurrent writes to complete
            assertTrue(writesCompleted.await(30, TimeUnit.SECONDS), "Concurrent writes should complete");

            List<SegmentAnalysis> items = volume.analyze();
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();

                Path originalSegmentFile = Segment.getSegmentFilePath(
                        volume.getConfig().dataDir(), segmentId
                );

                // Goal: verify that the SHA1 of the replicated segment file under destination
                // matches the SHA1 of the original segment file within a 15-second window.
                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    // SHA1 of the original segment file at this moment
                    byte[] expected = sha1(originalSegmentFile.toString());

                    // Iterate through all volume directories/paths under destination
                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile =
                                    volumeDir.resolve(Segment.generateFileName(segmentId));

                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }

                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }
        } finally {
            replication.shutdown();
        }
    }
}
