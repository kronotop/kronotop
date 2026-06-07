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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.volume.segment.SegmentAnalysis;
import com.kronotop.volume.segment.SegmentUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class VacuumTest extends BaseVolumeIntegrationTest {

    private long fillAndSealSegment(Prefix prefix) throws IOException {
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

        List<SegmentAnalysis> analysis = volume.analyze();
        assertEquals(2, analysis.size());
        return analysis.getFirst().segmentId();
    }

    @Test
    void shouldCollectSinglePrefixAndPersistVacuumMetadata() throws IOException {
        // Behavior: vacuum collects prefixes from SEGMENT_STATS and writes vacuum metadata to FDB
        long sealedSegmentId = fillAndSealSegment(stashVolumeSyncerPrefix);
        VacuumContext ctx = new VacuumContext();
        MockEntryEvacuator evacuator = new MockEntryEvacuator(ctx);
        volume.vacuum(ctx, sealedSegmentId, evacuator);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());

            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, volumeSubspace, sealedSegmentId);
            assertNotNull(metadata);
            assertEquals(VacuumMetadataStatus.EVACUATING, metadata.status());
            assertTrue(metadata.startedAt() > 0);
            assertTrue(metadata.startedAt() <= System.currentTimeMillis());

            byte[] prefixKey = volumeSubspace.packVacuumSegmentMetadataPrefixCardinalityKey(sealedSegmentId, stashVolumeSyncerPrefix);
            byte[] prefixValue = tr.get(prefixKey).join();
            assertNotNull(prefixValue);

            int cardinality = ByteBuffer.wrap(prefixValue).order(ByteOrder.LITTLE_ENDIAN).getInt();
            assertTrue(cardinality > 0);
        }
    }

    @Test
    void shouldCollectMultiplePrefixesInOneSegment() throws IOException {
        // Behavior: vacuum discovers and records all prefixes in a segment
        Prefix alpha = new Prefix("alpha");
        Prefix beta = new Prefix("beta");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession sessionAlpha = new VolumeSession(tr, alpha);
            volume.append(sessionAlpha, getEntries(5));

            VolumeSession sessionBeta = new VolumeSession(tr, beta);
            volume.append(sessionBeta, getEntries(3));
            tr.commit().join();
        }

        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, alpha);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        List<SegmentAnalysis> analysis = volume.analyze();
        assertEquals(2, analysis.size());
        long sealedSegmentId = analysis.getFirst().segmentId();

        VacuumContext ctx = new VacuumContext();
        MockEntryEvacuator evacuator = new MockEntryEvacuator(ctx);
        volume.vacuum(ctx, sealedSegmentId, evacuator);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());

            // Verify segment-level metadata
            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, volumeSubspace, sealedSegmentId);
            assertNotNull(metadata);

            // Scan all prefix cardinality keys
            Range range = VacuumSegmentMetadataUtil.getVacuumPrefixRange(volumeSubspace, sealedSegmentId);
            List<KeyValue> prefixKeys = tr.getRange(range).asList().join();
            assertTrue(prefixKeys.size() >= 2);

            // Verify both prefix-level keys exist with correct cardinalities
            byte[] alphaValue = tr.get(volumeSubspace.packVacuumSegmentMetadataPrefixCardinalityKey(sealedSegmentId, alpha)).join();
            assertNotNull(alphaValue);
            int alphaCardinality = ByteBuffer.wrap(alphaValue).order(ByteOrder.LITTLE_ENDIAN).getInt();
            assertTrue(alphaCardinality >= 5);

            byte[] betaValue = tr.get(volumeSubspace.packVacuumSegmentMetadataPrefixCardinalityKey(sealedSegmentId, beta)).join();
            assertNotNull(betaValue);
            int betaCardinality = ByteBuffer.wrap(betaValue).order(ByteOrder.LITTLE_ENDIAN).getInt();
            assertEquals(3, betaCardinality);
        }
    }

    @Test
    void shouldEvacuateEntriesFromAllPrefixes() throws IOException {
        // Behavior: vacuum loop iterates through all prefixes and calls evacuator for each prefix's entries
        Prefix alpha = new Prefix("alpha");
        Prefix beta = new Prefix("beta");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession sessionAlpha = new VolumeSession(tr, alpha);
            volume.append(sessionAlpha, getEntries(5));

            VolumeSession sessionBeta = new VolumeSession(tr, beta);
            volume.append(sessionBeta, getEntries(3));
            tr.commit().join();
        }

        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, alpha);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        List<SegmentAnalysis> analysis = volume.analyze();
        assertEquals(2, analysis.size());
        long sealedSegmentId = analysis.getFirst().segmentId();

        VacuumContext ctx = new VacuumContext();
        MockEntryEvacuator evacuator = new MockEntryEvacuator(ctx);
        volume.vacuum(ctx, sealedSegmentId, evacuator);

        assertFalse(evacuator.evacuated.isEmpty());

        Map<Prefix, Long> countByPrefix = evacuator.evacuated.stream()
                .collect(Collectors.groupingBy(
                        m -> Prefix.fromBytes(m.prefix()),
                        Collectors.counting()
                ));

        assertTrue(countByPrefix.containsKey(alpha));
        assertTrue(countByPrefix.containsKey(beta));
        assertTrue(countByPrefix.get(alpha) >= 5);
        assertEquals(3, countByPrefix.get(beta));
    }

    @Test
    void shouldRefuseToVacuumWritableSegment() {
        // Behavior: vacuum throws when targeting the currently writable segment
        VacuumContext ctx = new VacuumContext();
        MockEntryEvacuator evacuator = new MockEntryEvacuator(ctx);
        List<SegmentAnalysis> analysis = volume.analyze();
        assertEquals(1, analysis.size());
        long writableSegmentId = analysis.getFirst().segmentId();

        assertThrows(VacuumWritableSegmentException.class, () -> volume.vacuum(ctx, writableSegmentId, evacuator));
    }

    @Test
    void shouldMatchCardinalityWithSegmentStats() throws IOException {
        // Behavior: vacuum metadata cardinality exactly matches SEGMENT_STATS cardinality
        long sealedSegmentId = fillAndSealSegment(stashVolumeSyncerPrefix);

        VacuumContext ctx = new VacuumContext();
        MockEntryEvacuator evacuator = new MockEntryEvacuator(ctx);
        volume.vacuum(ctx, sealedSegmentId, evacuator);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());

            // Read cardinality from SEGMENT_STATS
            byte[] statsKey = volumeSubspace.packSegmentStatsKey(
                    sealedSegmentId, stashVolumeSyncerPrefix, SegmentStatsSubspaces.CARDINALITY);
            byte[] statsValue = tr.get(statsKey).join();
            assertNotNull(statsValue);
            int statsCardinality = ByteBuffer.wrap(statsValue).order(ByteOrder.LITTLE_ENDIAN).getInt();

            // Read cardinality from VACUUM_METADATA
            byte[] vacuumKey = volumeSubspace.packVacuumSegmentMetadataPrefixCardinalityKey(sealedSegmentId, stashVolumeSyncerPrefix);
            byte[] vacuumValue = tr.get(vacuumKey).join();
            assertNotNull(vacuumValue);
            int vacuumCardinality = ByteBuffer.wrap(vacuumValue).order(ByteOrder.LITTLE_ENDIAN).getInt();

            assertEquals(statsCardinality, vacuumCardinality);
        }
    }

    @Test
    void shouldPersistVacuumMetadataAcrossVolumeReopen() throws IOException {
        // Behavior: vacuum metadata survives volume close and reopen
        long sealedSegmentId = fillAndSealSegment(stashVolumeSyncerPrefix);

        VacuumContext ctx = new VacuumContext();
        MockEntryEvacuator evacuator = new MockEntryEvacuator(ctx);
        volume.vacuum(ctx, sealedSegmentId, evacuator);

        VolumeConfig volumeConfig = volume.getConfig();
        volume.close();
        volume = service.newVolume(volumeConfig);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volumeConfig.subspace());

            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, volumeSubspace, sealedSegmentId);
            assertNotNull(metadata);
            assertEquals(VacuumMetadataStatus.EVACUATING, metadata.status());
            assertTrue(metadata.startedAt() > 0);

            byte[] prefixKey = volumeSubspace.packVacuumSegmentMetadataPrefixCardinalityKey(sealedSegmentId, stashVolumeSyncerPrefix);
            byte[] prefixValue = tr.get(prefixKey).join();
            assertNotNull(prefixValue);

            int cardinality = ByteBuffer.wrap(prefixValue).order(ByteOrder.LITTLE_ENDIAN).getInt();
            assertTrue(cardinality > 0);
        }
    }

    @Test
    void shouldCleanupSegmentAfterSuccessfulVacuum() throws IOException {
        // Behavior: after vacuum evacuates all entries, segment metadata, file, and registry are cleaned up
        long sealedSegmentId = fillAndSealSegment(stashVolumeSyncerPrefix);

        VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, volumeSubspace);
            assertTrue(segmentIds.contains(sealedSegmentId));
        }

        VacuumContext ctx = new VacuumContext();
        EvacuatingEvacuator evacuator = new EvacuatingEvacuator(volume);
        volume.vacuum(ctx, sealedSegmentId, evacuator);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> posEntries = tr.getRange(Range.startsWith(volumeSubspace.packSegmentPositionPrefix(sealedSegmentId))).asList().join();
            assertTrue(posEntries.isEmpty());

            List<KeyValue> statsEntries = tr.getRange(Range.startsWith(volumeSubspace.packSegmentStatsSegmentPrefix(sealedSegmentId))).asList().join();
            assertTrue(statsEntries.isEmpty());

            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, volumeSubspace);
            assertFalse(segmentIds.contains(sealedSegmentId));

            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, volumeSubspace, sealedSegmentId);
            assertNull(metadata);
        }

        Path segmentPath = SegmentUtil.getFilePath(volume.getConfig().dataDir(), sealedSegmentId);
        assertFalse(Files.exists(segmentPath));
    }

    @Test
    void shouldNotCleanupSegmentWhenVacuumStopped() throws IOException {
        // Behavior: stopped vacuum leaves segment intact
        long sealedSegmentId = fillAndSealSegment(stashVolumeSyncerPrefix);

        VacuumContext ctx = new VacuumContext();
        ctx.stop();
        MockEntryEvacuator evacuator = new MockEntryEvacuator(ctx);
        volume.vacuum(ctx, sealedSegmentId, evacuator);

        VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, volumeSubspace);
            assertTrue(segmentIds.contains(sealedSegmentId));
        }

        Path segmentPath = SegmentUtil.getFilePath(volume.getConfig().dataDir(), sealedSegmentId);
        assertTrue(Files.exists(segmentPath));
    }

    @Test
    void shouldContinueServingOtherSegmentsAfterCleanup() throws IOException {
        // Behavior: cleaning up one segment does not affect reads from other segments
        long sealedSegmentId = fillAndSealSegment(stashVolumeSyncerPrefix);

        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, getEntries(5));
            tr.commit().join();
        }
        Versionstamp[] writableKeys = appendResult.getVersionstampedKeys();

        VacuumContext ctx = new VacuumContext();
        EvacuatingEvacuator evacuator = new EvacuatingEvacuator(volume);
        volume.vacuum(ctx, sealedSegmentId, evacuator);

        VolumeSession readSession = new VolumeSession(stashVolumeSyncerPrefix);
        for (Versionstamp key : writableKeys) {
            ByteBuffer data = volume.get(readSession, key);
            assertNotNull(data);
        }
    }

    @Test
    void shouldRefuseToCleanupSegmentWithRemainingEntries() throws IOException {
        // Behavior: cleanupSegment throws when the segment still has entries
        long sealedSegmentId = fillAndSealSegment(stashVolumeSyncerPrefix);
        assertThrows(IllegalStateException.class, () -> volume.destroyStaleSegment(sealedSegmentId));
    }

    static class MockEntryEvacuator implements EntryEvacuator {
        private final VacuumContext ctx;
        private final Set<Versionstamp> seen = new HashSet<>();
        private final List<EntryMetadata> evacuated = new ArrayList<>();

        MockEntryEvacuator(VacuumContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public boolean evacuate(Context context, Transaction tr, EntryMetadata metadata, Versionstamp versionstamp) {
            evacuated.add(metadata);
            if (!seen.add(versionstamp)) {
                ctx.stop();
            }
            return true;
        }
    }

    static class EvacuatingEvacuator implements EntryEvacuator {
        private final Volume volume;

        EvacuatingEvacuator(Volume volume) {
            this.volume = volume;
        }

        @Override
        public boolean evacuate(
                Context context,
                Transaction tr,
                EntryMetadata metadata,
                Versionstamp versionstamp
        ) throws IOException {
            ByteBuffer data = volume.getByEntryMetadata(metadata);
            Prefix prefix = Prefix.fromBytes(metadata.prefix());
            VolumeSession session = new VolumeSession(tr, prefix);
            VersionstampedEntryUpdate update = new VersionstampedEntryUpdate(versionstamp, metadata, data);
            volume.updateByVersionstampedEntryUpdate(session, update);
            return true;
        }
    }
}
