/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.base.Strings;
import com.kronotop.TestUtil;
import com.kronotop.cluster.client.protocol.ChangeLogEntryResponse;
import com.kronotop.cluster.client.protocol.ChangeLogRangeArgs;
import com.kronotop.cluster.client.protocol.VolumeInspectCursorResponse;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.*;
import com.kronotop.volume.segment.SegmentAnalysis;
import com.kronotop.volume.segment.SegmentUtil;
import io.lettuce.core.ClientOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VolumeReplicationTest extends BaseNetworkedVolumeIntegrationTest {

    private void replicationStopTrigger(VolumeReplication replication, DirectorySubspace standbySubspace) {
        Thread.ofVirtual().start(() -> await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                if (ReplicationState.readStage(tr, standbySubspace, 1L) == Stage.CHANGE_DATA_CAPTURE) {
                    replication.shutdown();
                    return true;
                }
                return false;
            }
        }));
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

    private Versionstamp[] insertEntries(int number, int length) throws IOException, VersionstampAlreadyExistsException {
        KeyEntry[] pairs = new KeyEntry[number];
        for (int i = 0; i < number; i++) {
            byte[] data = Strings.padStart(Integer.toString(i), length, '0').getBytes();
            pairs[i] = new KeyEntry(TestUtil.generateVersionstamp(i), ByteBuffer.wrap(data));
        }
        InsertResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            result = volume.insert(session, pairs);
            tr.commit().join();
        }
        result.complete();
        Versionstamp[] keys = new Versionstamp[number];
        for (int i = 0; i < number; i++) {
            keys[i] = result.entries()[i].versionstamp();
        }
        return keys;
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

            Path segmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), analysis.segmentId());
            byte[] expected = sha1(segmentFile.toString());
            try (var stream = Files.list(destination)) {
                stream.forEach(path -> {
                    Path replicatedSegmentFile = path.resolve(SegmentUtil.generateFileName(analysis.segmentId()));
                    byte[] actual = sha1(replicatedSegmentFile.toString());
                    assertArrayEquals(expected, actual);
                });
            }

            ReplicationStatus status = TransactionUtil.execute(context, tr ->
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

            Path segmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), analysis.segmentId());
            byte[] expected = sha1(segmentFile.toString());
            try (var stream = Files.list(destination)) {
                stream.forEach(path -> {
                    Path replicatedSegmentFile = path.resolve(SegmentUtil.generateFileName(analysis.segmentId()));
                    byte[] actual = sha1(replicatedSegmentFile.toString());
                    assertArrayEquals(expected, actual);
                });
            }
            ReplicationStatus status = TransactionUtil.execute(context, tr ->
                    ReplicationState.readStatus(tr, standbySubspace, analysis.segmentId())
            );
            assertEquals(ReplicationStatus.RUNNING, status);

            Stage stage = TransactionUtil.execute(context, tr ->
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
                List<Long> tailPointer = TransactionUtil.execute(context, tr ->
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

        // CDC trigger: write extra entries when the CHANGE_DATA_CAPTURE stage is reached
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

                Path originalSegmentFile = SegmentUtil.getFilePath(
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
                                    volumeDir.resolve(SegmentUtil.generateFileName(segmentId));

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

        // First replication run - stop after the first segment is replicated
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

        // Capture position after the first run
        long positionAfterFirstRun = TransactionUtil.execute(context, tr ->
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

            // Re-analyze volume after writes complete to get the current state
            List<SegmentAnalysis> currentItems = volume.analyze();

            // Verify segments converge to exact match
            for (SegmentAnalysis analysis : currentItems) {
                long segmentId = analysis.segmentId();
                Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());

                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));

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
        // Start with a fresh volume - default segment exists but has no entries
        List<SegmentAnalysis> initialItems = volume.analyze();
        assertEquals(1, initialItems.size(), "Fresh volume should have one default segment");
        assertEquals(0, initialItems.getFirst().cardinality(), "Default segment should have zero entries");

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
                Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());

                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));

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
            // Wait for the SEGMENT_REPLICATION stage on the first segment
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, fullSegmentId) == Stage.SEGMENT_REPLICATION;
                }
            });
            segmentReplicationSeen.countDown();

            // Wait for the first segment to be DONE
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

            // Verify transition to the CHANGE_DATA_CAPTURE stage
            assertTrue(cdcSeen.await(15, TimeUnit.SECONDS),
                    "CHANGE_DATA_CAPTURE stage should be reached after SEGMENT_REPLICATION");

            // Verify first segment final state
            Stage firstSegmentStage = TransactionUtil.execute(context, tr ->
                    ReplicationState.readStage(tr, standbySubspace, fullSegmentId)
            );
            assertEquals(Stage.SEGMENT_REPLICATION, firstSegmentStage);

            ReplicationStatus firstSegmentStatus = TransactionUtil.execute(context, tr ->
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
            // Wait for CDC stage on an initial segment
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
                Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());

                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));

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
                ReplicationStatus status = TransactionUtil.execute(context, tr ->
                        ReplicationState.readStatus(tr, standbySubspace, segmentId)
                );
                assertEquals(ReplicationStatus.DONE, status,
                        "Segment " + segmentId + " should have DONE status");

                // Verify stage is SEGMENT_REPLICATION (full segments stay in this stage)
                Stage stage = TransactionUtil.execute(context, tr ->
                        ReplicationState.readStage(tr, standbySubspace, segmentId)
                );
                assertEquals(Stage.SEGMENT_REPLICATION, stage,
                        "Segment " + segmentId + " should have SEGMENT_REPLICATION stage");

                // Verify checksum match
                Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);
                byte[] expected = sha1(originalSegmentFile.toString());

                try (var stream = Files.list(destination)) {
                    boolean matched = stream.anyMatch(volumeDir -> {
                        Path replicatedSegmentFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));
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

        // CDC trigger: write extra entries when the CHANGE_DATA_CAPTURE stage is reached
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

        // Replication thread, works at the background
        Thread.ofVirtual().start(replication::start);

        try {
            // Analyze original volume: expecting 17 segments
            await().atMost(Duration.ofSeconds(15)).until(() -> volume.analyze().size() == 17);

            List<SegmentAnalysis> items = volume.analyze();
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();

                Path originalSegmentFile = SegmentUtil.getFilePath(
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
                                    volumeDir.resolve(SegmentUtil.generateFileName(segmentId));

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

        // Replication thread - runs in the background and waits for new events in CDC mode
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

                    // Verify the sequence number and position match
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
            // Wait for segment 0 to enter the SEGMENT_REPLICATION stage
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

                Path originalSegmentFile = SegmentUtil.getFilePath(
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
                                    volumeDir.resolve(SegmentUtil.generateFileName(segmentId));

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
    void shouldReplicateMixedOperationsWithOutOfOrderVersionstamps(@TempDir Path destination) throws Exception {
        // Behavior: Appends entries, then concurrently performs INSERT (with older versionstamps),
        // UPDATE, and DELETE during replication. Verifies segment convergence and data integrity
        // on the replicated volume.
        int length = 1024;
        int number = 10;
        ByteBuffer[] entries = getEntries(number, length);

        // Append entries starting at userVersion=3, reserving 0..2 for INSERT versionstamps
        int insertCount = 3;
        Versionstamp[] appendedKeys;
        final int[] uv = {insertCount};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix, () -> uv[0]++);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            appendedKeys = appendResult.getVersionstampedKeys();
        }
        assertEquals(number, appendedKeys.length);

        // Fabricate INSERT versionstamps: same trVersion as appended keys but lower userVersions (0, 1, 2)
        byte[] trVersion = appendedKeys[0].getTransactionVersion();
        Versionstamp[] insertKeys = new Versionstamp[insertCount];
        for (int i = 0; i < insertCount; i++) {
            insertKeys[i] = Versionstamp.complete(trVersion, i);
        }

        // Assert precondition: insert versionstamps are lexicographically older than appended ones
        for (Versionstamp insertKey : insertKeys) {
            assertTrue(insertKey.compareTo(appendedKeys[0]) < 0,
                    "INSERT versionstamp should be older than first appended key");
        }

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        CountDownLatch mixedOpsCompletedLatch = new CountDownLatch(1);

        Versionstamp[] capturedAppendedKeys = appendedKeys;
        Thread.ofVirtual().start(() -> {
            try {
                // INSERT 3 entries with older versionstamps
                KeyEntry[] insertPairs = new KeyEntry[3];
                for (int i = 0; i < 3; i++) {
                    byte[] data = Strings.padStart("INSERT" + i, length, '0').getBytes();
                    insertPairs[i] = new KeyEntry(insertKeys[i], ByteBuffer.wrap(data));
                }
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    VolumeSession session = new VolumeSession(tr, prefix);
                    InsertResult insertResult = volume.insert(session, insertPairs);
                    tr.commit().join();
                    insertResult.complete();
                }

                // UPDATE appendedKeys[0] with new data
                byte[] updatedData = Strings.padStart("UPDATED", length, '0').getBytes();
                KeyEntry updatePair = new KeyEntry(capturedAppendedKeys[0], ByteBuffer.wrap(updatedData));
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    VolumeSession session = new VolumeSession(tr, prefix);
                    UpdateResult updateResult = volume.update(session, updatePair);
                    tr.commit().join();
                    updateResult.complete();
                }

                // DELETE appendedKeys[1]
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    VolumeSession session = new VolumeSession(tr, prefix);
                    volume.delete(session, capturedAppendedKeys[1]);
                    tr.commit().join();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            mixedOpsCompletedLatch.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            // Wait for mixed ops to complete
            assertTrue(mixedOpsCompletedLatch.await(15, TimeUnit.SECONDS));

            List<SegmentAnalysis> items = volume.analyze();
            assertFalse(items.isEmpty());

            // Wait for all segments to enter CDC mode
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();
                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    try (Transaction tr = context.getFoundationDB().createTransaction()) {
                        return ReplicationState.readStage(tr, standbySubspace, segmentId) == Stage.CHANGE_DATA_CAPTURE;
                    }
                });
            }

            // Wait for SHA-1 convergence per segment
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();
                Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());
                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));
                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }
                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }

            // Open replicated Volume and verify data integrity
            VolumeConfig replicatedVolumeConfig = new VolumeConfig(
                    volumeConfig.subspace(),
                    volumeConfig.name(),
                    destination.toString(),
                    volumeConfig.segmentSize()
            );
            Volume replicatedVolume = new Volume(context, replicatedVolumeConfig);

            // Verify appendedKeys[0] (updated) returns new data
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                ByteBuffer result = replicatedVolume.get(session, appendedKeys[0]);
                assertNotNull(result, "Updated key should return data");
                byte[] expectedData = Strings.padStart("UPDATED", length, '0').getBytes();
                assertEquals(ByteBuffer.wrap(expectedData), result);
            }

            // Verify appendedKeys[1] (deleted) returns null
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                ByteBuffer result = replicatedVolume.get(session, appendedKeys[1]);
                assertNull(result, "Deleted key should return null");
            }

            // Verify appendedKeys[2..9] (surviving) return original data
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 2; i < number; i++) {
                    ByteBuffer result = replicatedVolume.get(session, appendedKeys[i]);
                    assertNotNull(result, "Surviving key at index " + i + " should return data");
                    byte[] expectedData = Strings.padStart(Integer.toString(i), length, '0').getBytes();
                    assertEquals(ByteBuffer.wrap(expectedData), result);
                }
            }

            // Verify insertKeys[0..2] (older versionstamps) return their respective data
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 0; i < 3; i++) {
                    ByteBuffer result = replicatedVolume.get(session, insertKeys[i]);
                    assertNotNull(result, "Inserted key at index " + i + " should return data");
                    byte[] expectedData = Strings.padStart("INSERT" + i, length, '0').getBytes();
                    assertEquals(ByteBuffer.wrap(expectedData), result);
                }
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateInsertedEntriesAndVerifyChecksum(@TempDir Path destination) throws Exception {
        // Behavior: Inserts entries via volume.insert() with known versionstamps, replicates to standby,
        // and verifies segment file checksums match between primary and standby.
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        insertEntries(number, length);

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();
        try {
            replicationStopTrigger(replication, standbySubspace);
            replication.start();

            List<SegmentAnalysis> items = volume.analyze();
            assertEquals(2, items.size());

            SegmentAnalysis analysis = items.stream().filter(i -> i.size() - i.usedBytes() >= length).findFirst().orElse(null);
            assertNotNull(analysis);

            Path segmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), analysis.segmentId());
            byte[] expected = sha1(segmentFile.toString());
            try (var stream = Files.list(destination)) {
                stream.forEach(path -> {
                    Path replicatedSegmentFile = path.resolve(SegmentUtil.generateFileName(analysis.segmentId()));
                    byte[] actual = sha1(replicatedSegmentFile.toString());
                    assertArrayEquals(expected, actual);
                });
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateInsertedEntriesAndReadFromStandby(@TempDir Path destination) throws Exception {
        // Behavior: Inserts entries via volume.insert(), replicates to standby, opens a new Volume
        // on the replicated data directory, reads each entry by its known versionstamp, and verifies
        // data matches.
        int length = 1024;
        int number = 100;
        Versionstamp[] versionstampedKeys = insertEntries(number, length);

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        Thread.ofVirtual().start(replication::start);

        try {
            List<SegmentAnalysis> items = volume.analyze();
            assertEquals(1, items.size());
            long segmentId = items.getFirst().segmentId();

            // Wait for the segment to enter CDC mode
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, segmentId) == Stage.CHANGE_DATA_CAPTURE;
                }
            });

            // Verify SHA-1 convergence
            Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                byte[] expectedChecksum = sha1(originalSegmentFile.toString());
                try (var stream = Files.list(destination)) {
                    return stream.anyMatch(volumeDir -> {
                        Path replicatedSegmentFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));
                        if (!Files.exists(replicatedSegmentFile)) {
                            return false;
                        }
                        byte[] actual = sha1(replicatedSegmentFile.toString());
                        return Arrays.equals(expectedChecksum, actual);
                    });
                }
            });

            // Open the replicated volume and read entries
            VolumeConfig replicatedVolumeConfig = new VolumeConfig(
                    volumeConfig.subspace(),
                    volumeConfig.name(),
                    destination.toString(),
                    volumeConfig.segmentSize()
            );
            Volume replicatedVolume = new Volume(context, replicatedVolumeConfig);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 0; i < number; i++) {
                    ByteBuffer result = replicatedVolume.get(session, versionstampedKeys[i]);
                    assertNotNull(result, "Entry at index " + i + " should be readable from replicated volume");
                    byte[] expectedData = Strings.padStart(Integer.toString(i), length, '0').getBytes();
                    assertEquals(ByteBuffer.wrap(expectedData), result);
                }
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateInsertedEntriesDuringCDC(@TempDir Path destination) throws Exception {
        // Behavior: Appends initial entries to enter CDC mode, then inserts additional entries via
        // volume.insert() during CDC, and verifies segment convergence.
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        CountDownLatch writesCompleted = new CountDownLatch(1);

        // CDC trigger: insert entries when the CHANGE_DATA_CAPTURE stage is reached
        Thread.ofVirtual().start(() -> {
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, 1L) == Stage.CHANGE_DATA_CAPTURE;
                }
            });

            for (int i = 0; i < 10; i++) {
                try {
                    insertEntries(1, 10);
                } catch (IOException | VersionstampAlreadyExistsException e) {
                    throw new RuntimeException(e);
                }
            }
            writesCompleted.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            await().atMost(Duration.ofSeconds(15)).until(() -> volume.analyze().size() == 2);
            assertTrue(writesCompleted.await(30, TimeUnit.SECONDS), "Concurrent writes should complete");

            List<SegmentAnalysis> items = volume.analyze();
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();

                Path originalSegmentFile = SegmentUtil.getFilePath(
                        volume.getConfig().dataDir(), segmentId
                );

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expectedChecksum = sha1(originalSegmentFile.toString());

                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile =
                                    volumeDir.resolve(SegmentUtil.generateFileName(segmentId));

                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }

                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expectedChecksum, actual);
                        });
                    }
                });
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateWithVolumeFacadeDeleteByVersionstampedEntry(@TempDir Path destination) throws Exception {
        // Behavior: Appends entries, deletes some via VolumeFacade.deleteByVersionstampedEntry during replication,
        // and verifies segment convergence plus data integrity on the replicated volume.
        int length = 1024;
        int number = 10;
        ByteBuffer[] entries = getEntries(number, length);

        Versionstamp[] appendedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            appendedKeys = appendResult.getVersionstampedKeys();
        }
        assertEquals(number, appendedKeys.length);

        // Collect VersionstampedEntry for the first 3 entries
        List<VersionstampedEntry> versionstampedEntries = new java.util.ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                versionstampedEntries.add(new VersionstampedEntry(volumeEntry.key(), EntryMetadata.decode(volumeEntry.metadata())));
                if (versionstampedEntries.size() == 3) break;
            }
        }
        assertEquals(3, versionstampedEntries.size());

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        CountDownLatch deletesCompletedLatch = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            // Delete the first 3 entries via VolumeFacade.deleteByVersionstampedEntry
            VolumeFacade facade = new VolumeFacade(context);
            VolumeSubspace volumeSubspace = new VolumeSubspace(volumeConfig.subspace());
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                facade.deleteByVersionstampedEntry(volumeSubspace, session, versionstampedEntries.toArray(new VersionstampedEntry[0]));
                tr.commit().join();
            }
            deletesCompletedLatch.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            assertTrue(deletesCompletedLatch.await(15, TimeUnit.SECONDS));

            List<SegmentAnalysis> items = volume.analyze();
            assertFalse(items.isEmpty());

            // Wait for all segments to enter CDC mode
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();
                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    try (Transaction tr = context.getFoundationDB().createTransaction()) {
                        return ReplicationState.readStage(tr, standbySubspace, segmentId) == Stage.CHANGE_DATA_CAPTURE;
                    }
                });
            }

            // Wait for SHA-1 convergence per segment
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();
                Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());
                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));
                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }
                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }

            // Open replicated Volume and verify data integrity
            VolumeConfig replicatedVolumeConfig = new VolumeConfig(
                    volumeConfig.subspace(),
                    volumeConfig.name(),
                    destination.toString(),
                    volumeConfig.segmentSize()
            );
            Volume replicatedVolume = new Volume(context, replicatedVolumeConfig);

            // Verify deleted keys (first 3) return null
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 0; i < 3; i++) {
                    ByteBuffer result = replicatedVolume.get(session, appendedKeys[i]);
                    assertNull(result, "Deleted key at index " + i + " should return null");
                }
            }

            // Verify surviving keys (3..9) return original data
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 3; i < number; i++) {
                    ByteBuffer result = replicatedVolume.get(session, appendedKeys[i]);
                    assertNotNull(result, "Surviving key at index " + i + " should return data");
                    byte[] expectedData = Strings.padStart(Integer.toString(i), length, '0').getBytes();
                    assertEquals(ByteBuffer.wrap(expectedData), result);
                }
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateWithVolumeUpdate(@TempDir Path destination) throws Exception {
        // Behavior: Appends entries, updates some via Volume.update during replication,
        // and verifies segment convergence plus data integrity on the replicated volume.
        int length = 1024;
        int number = 10;
        ByteBuffer[] entries = getEntries(number, length);

        Versionstamp[] appendedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            appendedKeys = appendResult.getVersionstampedKeys();
        }
        assertEquals(number, appendedKeys.length);

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        CountDownLatch updatesCompletedLatch = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            try {
                KeyEntry[] pairs = new KeyEntry[3];
                for (int i = 0; i < 3; i++) {
                    byte[] data = Strings.padStart("UPDATED" + i, length, '0').getBytes();
                    pairs[i] = new KeyEntry(appendedKeys[i], ByteBuffer.wrap(data));
                }
                UpdateResult updateResult;
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    VolumeSession session = new VolumeSession(tr, prefix);
                    updateResult = volume.update(session, pairs);
                    tr.commit().join();
                }
                updateResult.complete();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            updatesCompletedLatch.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            assertTrue(updatesCompletedLatch.await(15, TimeUnit.SECONDS));

            List<SegmentAnalysis> items = volume.analyze();
            assertFalse(items.isEmpty());

            // Wait for all segments to enter CDC mode
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();
                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    try (Transaction tr = context.getFoundationDB().createTransaction()) {
                        return ReplicationState.readStage(tr, standbySubspace, segmentId) == Stage.CHANGE_DATA_CAPTURE;
                    }
                });
            }

            // Wait for SHA-1 convergence per segment
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();
                Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());
                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));
                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }
                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }

            // Open replicated Volume and verify data integrity
            VolumeConfig replicatedVolumeConfig = new VolumeConfig(
                    volumeConfig.subspace(),
                    volumeConfig.name(),
                    destination.toString(),
                    volumeConfig.segmentSize()
            );
            Volume replicatedVolume = new Volume(context, replicatedVolumeConfig);

            // Verify updated keys (first 3) return new data
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 0; i < 3; i++) {
                    ByteBuffer result = replicatedVolume.get(session, appendedKeys[i]);
                    assertNotNull(result, "Updated key at index " + i + " should return data");
                    byte[] expectedData = Strings.padStart("UPDATED" + i, length, '0').getBytes();
                    assertEquals(ByteBuffer.wrap(expectedData), result);
                }
            }

            // Verify untouched keys (3..9) return original data
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 3; i < number; i++) {
                    ByteBuffer result = replicatedVolume.get(session, appendedKeys[i]);
                    assertNotNull(result, "Untouched key at index " + i + " should return data");
                    byte[] expectedData = Strings.padStart(Integer.toString(i), length, '0').getBytes();
                    assertEquals(ByteBuffer.wrap(expectedData), result);
                }
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateAllEntriesUnderConcurrentWritersWithRandomCommitDelays(@TempDir Path destination) throws Exception {
        // Behavior: Spawns many concurrent writers with random commit delays during CDC to exercise the
        // safe watermark tracking. Verifies that the standby converges to an exact byte-for-byte copy
        // of every segment, proving no entries were skipped due to in-flight transaction races.
        int length = 1024;
        int entriesPerSegment = Math.toIntExact(volume.getConfig().segmentSize() / length);

        // 1. Fill one segment + overflow to trigger segment replication → CDC transition
        appendEntries(entriesPerSegment + (entriesPerSegment / 2), length);

        // 2. Start replication
        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();
        Thread.ofVirtual().start(replication::start);

        try {
            // 3. Wait for CDC stage on the active segment (segment 1)
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, 1L) == Stage.CHANGE_DATA_CAPTURE;
                }
            });

            // 4. Spawn concurrent writers with random commit delays
            int writerCount = 200;
            CountDownLatch writersFinished = new CountDownLatch(writerCount);
            Random random = new Random();

            for (int i = 0; i < writerCount; i++) {
                Thread.ofVirtual().start(() -> {
                    try {
                        ByteBuffer[] entries = getEntries(1, length);
                        try (Transaction tr = database.createTransaction()) {
                            VolumeSession session = new VolumeSession(tr, prefix);
                            volume.append(session, entries);

                            // Random delay before commit — creates the in-flight race window
                            Thread.sleep(random.nextInt(100));
                            tr.commit().join();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        writersFinished.countDown();
                    }
                });
            }

            // 5. Wait for all writers to finish
            assertTrue(writersFinished.await(30, TimeUnit.SECONDS), "Writers should finish within 30 seconds");

            // 6. Wait for standby to converge — verify SHA-1 match for all segments
            List<SegmentAnalysis> items = volume.analyze();
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();
                Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(30)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());
                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));
                            if (!Files.exists(replicatedFile)) return false;
                            byte[] actual = sha1(replicatedFile.toString());
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
    void shouldConvergeReplicationWhenSomeConcurrentWritersRollBack(@TempDir Path destination) throws Exception {
        // Behavior: Spawns concurrent writers where half commit and half roll back during CDC.
        // Verifies that rolled-back transactions properly clean up their in-flight sequence numbers,
        // allowing the safe watermark to advance and replication to converge with correct data.
        int length = 1024;
        int entriesPerSegment = Math.toIntExact(volume.getConfig().segmentSize() / length);

        // 1. Fill one segment + overflow → CDC on segment 1
        appendEntries(entriesPerSegment + (entriesPerSegment / 2), length);

        // 2. Start replication
        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();
        Thread.ofVirtual().start(replication::start);

        try {
            // 3. Wait for CDC on segment 1
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, 1L) == Stage.CHANGE_DATA_CAPTURE;
                }
            });

            // 4. Spawn concurrent writers — half commit, half roll back
            int writerCount = 200;
            CountDownLatch writersFinished = new CountDownLatch(writerCount);
            Random random = new Random();

            for (int i = 0; i < writerCount; i++) {
                final boolean shouldCommit = (i % 2 == 0);
                Thread.ofVirtual().start(() -> {
                    try {
                        ByteBuffer[] entries = getEntries(1, length);
                        try (Transaction tr = database.createTransaction()) {
                            VolumeSession session = new VolumeSession(tr, prefix);
                            volume.append(session, entries);

                            // Random delay — creates the in-flight race window
                            Thread.sleep(random.nextInt(100));

                            if (shouldCommit) {
                                tr.commit().join();
                            } else {
                                tr.cancel();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        writersFinished.countDown();
                    }
                });
            }

            // 5. Wait for all writers
            assertTrue(writersFinished.await(30, TimeUnit.SECONDS), "Writers should finish within 30 seconds");

            // 6. Append sentinel entries to advance watermark past any lagging state
            appendEntries(10, length);

            // 7. Wait for convergence via CDC sequence number and position
            ReplicationClient replicationClient = new ReplicationClient(context, SHARD_KIND, SHARD_ID);
            replicationClient.connect(ClientOptions.create());
            try {
                await().atMost(Duration.ofSeconds(30)).until(() -> {
                    VolumeInspectCursorResponse primaryCursor =
                            replicationClient.conn().sync().volumeInspectCursor(volumeConfig.name());
                    if (!primaryCursor.hasData()) return false;

                    try (Transaction tr = context.getFoundationDB().createTransaction()) {
                        Stage stage = ReplicationState.readStage(tr, standbySubspace, primaryCursor.activeSegmentId());
                        if (stage != Stage.CHANGE_DATA_CAPTURE) return false;

                        long standbySequenceNumber = ReplicationState.readSequenceNumber(
                                tr, standbySubspace, primaryCursor.activeSegmentId());
                        long standbyPosition = ReplicationState.readPosition(
                                tr, standbySubspace, primaryCursor.activeSegmentId());

                        return standbySequenceNumber == primaryCursor.sequenceNumber()
                                && standbyPosition == primaryCursor.nextPosition();
                    }
                });
            } finally {
                replicationClient.shutdown();
            }

            // 8. Verify data integrity — every committed entry readable from standby
            VolumeConfig replicatedVolumeConfig = new VolumeConfig(
                    volumeConfig.subspace(),
                    volumeConfig.name(),
                    destination.toString(),
                    volumeConfig.segmentSize()
            );
            Volume replicatedVolume = new Volume(context, replicatedVolumeConfig);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (VolumeEntry entry : volume.getRange(session)) {
                    ByteBuffer standbyData = replicatedVolume.get(session, entry.key());
                    assertNotNull(standbyData, "Entry should be readable from standby");
                    assertEquals(entry.entry(), standbyData, "Data should match between primary and standby");
                }
            }
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldReplicateWithVolumeDeleteByVersionstampedEntry(@TempDir Path destination) throws Exception {
        // Behavior: Appends entries, deletes some via Volume.deleteByVersionstampedEntry during replication,
        // and verifies segment convergence plus data integrity on the replicated volume.
        int length = 1024;
        int number = 10;
        ByteBuffer[] entries = getEntries(number, length);

        Versionstamp[] appendedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult appendResult = volume.append(session, entries);
            tr.commit().join();
            appendedKeys = appendResult.getVersionstampedKeys();
        }
        assertEquals(number, appendedKeys.length);

        // Collect VersionstampedEntry for the first 3 entries
        List<VersionstampedEntry> versionstampedEntries = new java.util.ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                versionstampedEntries.add(new VersionstampedEntry(volumeEntry.key(), EntryMetadata.decode(volumeEntry.metadata())));
                if (versionstampedEntries.size() == 3) break;
            }
        }
        assertEquals(3, versionstampedEntries.size());

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        CountDownLatch deletesCompletedLatch = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            DeleteResult deleteResult;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                deleteResult = volume.deleteByVersionstampedEntry(session, versionstampedEntries.toArray(new VersionstampedEntry[0]));
                tr.commit().join();
            }
            deleteResult.complete();
            deletesCompletedLatch.countDown();
        });

        // Replication thread
        Thread.ofVirtual().start(replication::start);

        try {
            assertTrue(deletesCompletedLatch.await(15, TimeUnit.SECONDS));

            List<SegmentAnalysis> items = volume.analyze();
            assertFalse(items.isEmpty());

            // Wait for all segments to enter CDC mode
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();
                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    try (Transaction tr = context.getFoundationDB().createTransaction()) {
                        return ReplicationState.readStage(tr, standbySubspace, segmentId) == Stage.CHANGE_DATA_CAPTURE;
                    }
                });
            }

            // Wait for SHA-1 convergence per segment
            for (SegmentAnalysis analysis : items) {
                long segmentId = analysis.segmentId();
                Path originalSegmentFile = SegmentUtil.getFilePath(volume.getConfig().dataDir(), segmentId);

                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    byte[] expected = sha1(originalSegmentFile.toString());
                    try (var stream = Files.list(destination)) {
                        return stream.anyMatch(volumeDir -> {
                            Path replicatedSegmentFile = volumeDir.resolve(SegmentUtil.generateFileName(segmentId));
                            if (!Files.exists(replicatedSegmentFile)) {
                                return false;
                            }
                            byte[] actual = sha1(replicatedSegmentFile.toString());
                            return Arrays.equals(expected, actual);
                        });
                    }
                });
            }

            // Open replicated Volume and verify data integrity
            VolumeConfig replicatedVolumeConfig = new VolumeConfig(
                    volumeConfig.subspace(),
                    volumeConfig.name(),
                    destination.toString(),
                    volumeConfig.segmentSize()
            );
            Volume replicatedVolume = new Volume(context, replicatedVolumeConfig);

            // Verify deleted keys (first 3) return null
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 0; i < 3; i++) {
                    ByteBuffer result = replicatedVolume.get(session, appendedKeys[i]);
                    assertNull(result, "Deleted key at index " + i + " should return null");
                }
            }

            // Verify surviving keys (3..9) return original data
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                for (int i = 3; i < number; i++) {
                    ByteBuffer result = replicatedVolume.get(session, appendedKeys[i]);
                    assertNotNull(result, "Surviving key at index " + i + " should return data");
                    byte[] expectedData = Strings.padStart(Integer.toString(i), length, '0').getBytes();
                    assertEquals(ByteBuffer.wrap(expectedData), result);
                }
            }
        } finally {
            replication.shutdown();
        }
    }

    private byte[] readRange(Path file, long position, int length) throws IOException {
        byte[] buf = new byte[length];
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            raf.seek(position);
            raf.readFully(buf);
        }
        return buf;
    }

    @Test
    void shouldRolloverSegmentFileWhenBatchStartsInNextSegment(@TempDir Path destination)
            throws IOException, java.util.concurrent.ExecutionException, InterruptedException {
        // Behavior: When a CDC batch's first entry already belongs to a segment ahead of the
        // checkpoint (ranges-empty / segment-only-advance path), processChanges must reopen the
        // segment file before advancing the checkpoint. Otherwise next-iteration bytes for the new
        // segment are written into the old segment file (silent standby corruption). This drives
        // processChanges directly with a segment-1-only changes list while the checkpoint is on
        // segment 0, then asserts segment-1 bytes land in the segment-1 file and segment 0 is
        // left untouched.
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);
        // 1 full segment (0) + 1 partial segment (1)
        assertEquals(2, volume.analyze().size());

        ReplicationClient client = new ReplicationClient(context, SHARD_KIND, SHARD_ID);
        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        ChangeDataCapture cdc = null;
        try {
            client.connect(ClientOptions.create());

            // Read the real changelog so the segment-1 coordinates are valid and SEGMENT.RANGE
            // returns the correct bytes for the next-iteration pull.
            long latestSequenceNumber = client.conn().sync().changelogWatch(volumeConfig.name(), 0L);
            List<ChangeLogEntryResponse> allChanges = client.conn().sync().changelogRange(
                    volumeConfig.name(), ParentOperationKind.LIFECYCLE.toString(), "[0", latestSequenceNumber + "]",
                    ChangeLogRangeArgs.Builder.limit(number + 100));

            List<ChangeLogEntryResponse> segment1Changes = new ArrayList<>();
            for (ChangeLogEntryResponse entry : allChanges) {
                if (entry.after() != null && entry.after().segmentId() == 1L) {
                    segment1Changes.add(entry);
                }
            }
            assertFalse(segment1Changes.isEmpty(), "Expected at least one changelog entry in segment 1");

            DirectorySubspace subspace = replication.createOrOpenStandbySubspace();
            long segmentSize = volume.getConfig().segmentSize();
            // chunkSize large enough that the batch is never split by chunk size, only by segment.
            ReplicationSession session = new ReplicationSession(
                    volumeConfig.name(), SHARD_KIND, SHARD_ID, destination,
                    0L, 0L, 16L * 1024 * 1024, segmentSize);
            // Constructing CDC opens segment 0's file on the standby (the file that must NOT
            // receive segment-1 bytes).
            cdc = new ChangeDataCapture(context, subspace, client, session);

            Path standbySegment0 = SegmentUtil.getFilePath(destination.toAbsolutePath().toString(), 0L);
            byte[] segment0Before = sha1(standbySegment0.toString());

            ChangeDataCapture.Checkpoint checkpoint = new ChangeDataCapture.Checkpoint();
            checkpoint.setSegmentId(0L);
            checkpoint.setSequenceNumber(0L);

            cdc.processChanges(checkpoint, latestSequenceNumber, segment1Changes);

            // Checkpoint advanced to the new segment.
            assertEquals(1L, checkpoint.segmentId());

            // Segment 1 bytes must have landed in the standby's segment-1 file.
            Path standbySegment1 = SegmentUtil.getFilePath(destination.toAbsolutePath().toString(), 1L);
            assertTrue(Files.exists(standbySegment1), "Standby segment-1 file was never created");

            Path primarySegment1 = SegmentUtil.getFilePath(volume.getConfig().dataDir(), 1L);
            for (ChangeLogEntryResponse entry : segment1Changes) {
                long position = entry.after().position();
                int len = Math.toIntExact(entry.after().length());
                byte[] expected = readRange(primarySegment1, position, len);
                byte[] actual = readRange(standbySegment1, position, len);
                assertArrayEquals(expected, actual,
                        "Segment-1 bytes at position " + position + " were not written to the segment-1 file");
            }

            // Segment 0 must be untouched (no segment-1 bytes leaked into it).
            byte[] segment0After = sha1(standbySegment0.toString());
            assertArrayEquals(segment0Before, segment0After, "Segment-0 file was corrupted by the rollover");
        } finally {
            if (cdc != null) {
                cdc.close();
            }
            client.shutdown();
            replication.shutdown();
        }
    }

    @Test
    void shouldResumeWhenSegmentStatusIsFailed(@TempDir Path destination) {
        // Behavior: a FAILED segment resumes from its cursor instead of permanently stopping replication.
        VolumeReplication replication =
                new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        long segmentId = 1L;
        long position = 4096L;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setPosition(tr, standbySubspace, segmentId, position);
            ReplicationState.setStage(tr, standbySubspace, segmentId, Stage.SEGMENT_REPLICATION);
            ReplicationState.setStatus(tr, standbySubspace, segmentId, ReplicationStatus.FAILED);
            tr.commit().join();
        }

        VolumeReplication.ReplicationStep step = replication.resolveNextSegment();

        assertFalse(step.stop());
        assertEquals(segmentId, step.segmentId());
        assertEquals(position, step.position());
        assertFalse(step.startCDC());
    }

    @Test
    void shouldStopWhenSegmentStatusIsStopped(@TempDir Path destination) {
        // Behavior: a STOPPED segment is terminal and halts replication.
        VolumeReplication replication =
                new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();

        long segmentId = 1L;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setPosition(tr, standbySubspace, segmentId, 0L);
            ReplicationState.setStatus(tr, standbySubspace, segmentId, ReplicationStatus.STOPPED);
            tr.commit().join();
        }

        VolumeReplication.ReplicationStep step = replication.resolveNextSegment();

        assertTrue(step.stop());
    }

    @Test
    void shouldTransitionToCdcOnEmptyActiveSegmentInsteadOfSpinning(@TempDir Path destination) throws IOException {
        // Behavior: an empty active segment that follows a data-bearing segment must hand off to
        // CDC instead of busy-spinning on no-op SegmentReplication. The primary commits a new
        // active segment's metadata before its first entry, so the standby can observe an empty
        // active segment. SegmentReplication on it is a no-op (limitPosition=0) and never writes a
        // POSITION key, so the cursor stays on the previous data-bearing segment. Without the fix,
        // resolveNextSegment keeps re-dispatching the no-op bulk forever and CDC is never reached.

        // Produce a data-bearing segment 0 on the primary that the standby can bulk-replicate.
        appendEntries(64, 1024);

        // Inject an empty, active segment 1 directly into the primary's volume metadata. This is
        // exactly what Volume.getOrCreateWritableSegment commits (segment-id key -> empty bytes)
        // before writing the first entry, reproducing the empty active-segment window.
        DirectorySubspace primarySubspace = volume.getConfig().subspace();
        byte[] segmentIdKey = primarySubspace.pack(Tuple.from(
                Subspaces.VOLUME_METADATA_SUBSPACE,
                VolumeMetadataField.SEGMENT_ID.getValue(),
                1L));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.set(segmentIdKey, new byte[0]);
            tr.commit().join();
        }

        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace standbySubspace = replication.createOrOpenStandbySubspace();
        try {
            Thread.ofVirtual().start(replication::start);

            // The standby bulk-replicates segment 0, reaches the empty active segment 1, and must
            // hand off to CDC rather than spinning on it.
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    return ReplicationState.readStage(tr, standbySubspace, 1L) == Stage.CHANGE_DATA_CAPTURE;
                }
            });
        } finally {
            replication.shutdown();
        }
    }

    @Test
    void shouldBeIdempotentOnSequentialDoubleClose(@TempDir Path destination) throws Exception {
        // Behavior: close() syncs and closes the segment file at most once. A second close() is a
        // no-op and must never sync an already-closed file descriptor (the double-close TOCTOU that
        // previously surfaced as a SyncFailedException). started==false here, so the close is not
        // gated by the drain timeout.
        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace subspace = replication.createOrOpenStandbySubspace();
        ReplicationClient client = new ReplicationClient(context, SHARD_KIND, SHARD_ID);
        client.connect(ClientOptions.create());
        ReplicationSession session = new ReplicationSession(
                volumeConfig.name(), SHARD_KIND, SHARD_ID, destination,
                0L, 0L, 1024L, volume.getConfig().segmentSize());
        SegmentReplication stage = new SegmentReplication(context, subspace, client, session);
        try {
            assertTrue(stage.file.getChannel().isOpen());

            stage.close();
            assertFalse(stage.file.getChannel().isOpen(), "First close must close the segment file");

            // Second close must be a no-op: no exception, file stays closed.
            assertDoesNotThrow(stage::close);
            assertFalse(stage.file.getChannel().isOpen());
        } finally {
            client.shutdown();
            replication.shutdown();
        }
    }

    @Test
    void shouldNotThrowWhenClosedConcurrentlyFromTwoThreads(@TempDir Path destination) throws Exception {
        // Behavior: the worker-loop teardown and the shutdown thread can close the same stage
        // concurrently. The fileLock-guarded one-shot close must prevent a sync on a closed
        // descriptor, so neither close() throws and the file ends closed. Looped to exercise the race.
        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        DirectorySubspace subspace = replication.createOrOpenStandbySubspace();
        ReplicationClient client = new ReplicationClient(context, SHARD_KIND, SHARD_ID);
        client.connect(ClientOptions.create());
        try {
            long segmentSize = volume.getConfig().segmentSize();
            for (int i = 0; i < 50; i++) {
                ReplicationSession session = new ReplicationSession(
                        volumeConfig.name(), SHARD_KIND, SHARD_ID, destination,
                        0L, 0L, 1024L, segmentSize);
                SegmentReplication stage = new SegmentReplication(context, subspace, client, session);

                CountDownLatch ready = new CountDownLatch(1);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                Runnable closer = () -> {
                    try {
                        ready.await();
                        stage.close();
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                };
                Thread t1 = new Thread(closer);
                Thread t2 = new Thread(closer);
                t1.start();
                t2.start();
                ready.countDown();
                t1.join();
                t2.join();

                assertNull(failure.get(), "Concurrent close() must not throw");
                assertFalse(stage.file.getChannel().isOpen(), "File must be closed after concurrent close()");
            }
        } finally {
            client.shutdown();
            replication.shutdown();
        }
    }

    @Test
    void shouldSkipFileCloseWhenWorkerNotDrained(@TempDir Path destination) throws Exception {
        // Behavior: when close() runs while the worker has not drained (the latch never counts down
        // within the grace period), close() must NOT touch the segment file, because the worker may
        // still be mid-rollover. It logs the warning and leaves the file open; the terminal close
        // happens on a later drained close().
        appendEntries(1, 1024); // keep the primary changelog endpoint live for the CDC client
        ReplicationClient client = new ReplicationClient(context, SHARD_KIND, SHARD_ID);
        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        ChangeDataCapture cdc = null;
        try {
            client.connect(ClientOptions.create());
            DirectorySubspace subspace = replication.createOrOpenStandbySubspace();
            ReplicationSession session = new ReplicationSession(
                    volumeConfig.name(), SHARD_KIND, SHARD_ID, destination,
                    0L, 0L, 1024L, volume.getConfig().segmentSize());
            cdc = new ChangeDataCapture(context, subspace, client, session);

            // Mark the stage as started without running it: the latch is never counted down, so
            // close() observes the worker as not drained and the grace period times out.
            cdc.started = true;

            cdc.close();
            assertTrue(cdc.file.getChannel().isOpen(),
                    "close() must not close the file while the worker is not drained");

            // Simulate the worker draining; the next close() performs the terminal close.
            cdc.latch.countDown();
            cdc.close();
            assertFalse(cdc.file.getChannel().isOpen(),
                    "A drained close() must perform the terminal file close");
        } finally {
            if (cdc != null) {
                cdc.close();
            }
            client.shutdown();
            replication.shutdown();
        }
    }

    @Test
    void shouldBackOffWhenClientNotConnectedDuringSegmentReplication(@TempDir Path destination) throws Exception {
        // Behavior: when the replication client is not connected to the primary, SegmentReplication
        // parks for the configured reconnection backoff before returning instead of busy-spinning, and
        // does not mark the segment as FAILED.
        long backoffMillis = context.getConfig().getLong("volume.replication.reconnect_backoff");

        // A fresh client that was never connected: conn() throws ReplicationClientNotConnectedException.
        ReplicationClient client = new ReplicationClient(context, SHARD_KIND, SHARD_ID);
        VolumeReplication replication = new VolumeReplication(context, SHARD_KIND, SHARD_ID, destination.toString());
        SegmentReplication stage = null;
        try {
            DirectorySubspace subspace = replication.createOrOpenStandbySubspace();
            ReplicationSession session = new ReplicationSession(
                    volumeConfig.name(), SHARD_KIND, SHARD_ID, destination,
                    0L, 0L, 1024L, volume.getConfig().segmentSize());
            stage = new SegmentReplication(context, subspace, client, session);

            long startedAt = System.nanoTime();
            stage.start(); // must swallow the not-connected exception, not throw
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

            // The stage backed off rather than returning instantly. The 50ms slack tolerates an early
            // park wakeup while still ruling out an immediate (busy-spin) return.
            assertTrue(elapsedMillis >= backoffMillis - 50,
                    "Expected backoff >= " + (backoffMillis - 50) + "ms, but start() returned after " + elapsedMillis + "ms");

            // The not-connected path must not mark the segment as failed.
            ReplicationStatus status = TransactionUtil.execute(context, tr ->
                    ReplicationState.readStatus(tr, subspace, session.segmentId()));
            assertNotEquals(ReplicationStatus.FAILED, status);
        } finally {
            if (stage != null) {
                stage.close();
            }
            client.shutdown();
            replication.shutdown();
        }
    }
}
