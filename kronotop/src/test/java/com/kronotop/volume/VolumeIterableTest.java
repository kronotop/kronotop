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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VolumeIterableTest extends BaseVolumeIntegrationTest {

    @Test
    void shouldIterateAllEntries() throws IOException {
        ByteBuffer[] entries = getEntries(5);

        Versionstamp[] versionstampedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = result.getVersionstampedKeys();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = volume.getRange(session);

            List<VolumeEntry> retrievedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : iterable) {
                retrievedEntries.add(volumeEntry);
            }

            assertEquals(5, retrievedEntries.size());

            for (int i = 0; i < 5; i++) {
                VolumeEntry volumeEntry = retrievedEntries.get(i);

                // Verify key matches
                assertEquals(versionstampedKeys[i], volumeEntry.key());

                // Verify entry data matches
                ByteBuffer expected = entries[i];
                expected.flip();
                assertArrayEquals(expected.array(), volumeEntry.entry().array());

                // Verify metadata is present and decodable
                assertNotNull(volumeEntry.metadata());
                EntryMetadata metadata = EntryMetadata.decode(volumeEntry.metadata());
                assertNotNull(metadata);
            }
        }
    }

    @Test
    void shouldReturnEmptyIteratorWhenNoEntries() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = volume.getRange(session);

            List<VolumeEntry> retrievedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : iterable) {
                retrievedEntries.add(volumeEntry);
            }

            assertTrue(retrievedEntries.isEmpty());
        }
    }

    @Test
    void shouldRespectLimitParameter() throws IOException {
        ByteBuffer[] entries = getEntries(5);

        Versionstamp[] versionstampedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = result.getVersionstampedKeys();
        }

        int limit = 2;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = volume.getRange(session, limit);

            List<VolumeEntry> retrievedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : iterable) {
                retrievedEntries.add(volumeEntry);
            }

            assertEquals(limit, retrievedEntries.size());

            // Verify the first 2 entries are returned
            for (int i = 0; i < limit; i++) {
                assertEquals(versionstampedKeys[i], retrievedEntries.get(i).key());
            }
        }
    }

    @Test
    void shouldIterateInReverseOrder() throws IOException {
        ByteBuffer[] entries = getEntries(3);

        Versionstamp[] versionstampedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = result.getVersionstampedKeys();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = volume.getRange(session, true);

            List<VolumeEntry> retrievedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : iterable) {
                retrievedEntries.add(volumeEntry);
            }

            assertEquals(3, retrievedEntries.size());

            // Verify descending order (last inserted should be first)
            assertEquals(versionstampedKeys[2], retrievedEntries.get(0).key());
            assertEquals(versionstampedKeys[1], retrievedEntries.get(1).key());
            assertEquals(versionstampedKeys[0], retrievedEntries.get(2).key());
        }
    }

    @Test
    void shouldFilterByVersionstampRange() throws IOException {
        ByteBuffer[] entries = getEntries(5);

        Versionstamp[] versionstampedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = result.getVersionstampedKeys();
        }

        // Filter to get entries at indices 1, 2 (index 1 to 3 exclusive)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(versionstampedKeys[1]);
            VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(versionstampedKeys[3]);
            Iterable<VolumeEntry> iterable = volume.getRange(session, begin, end);

            List<VolumeEntry> retrievedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : iterable) {
                retrievedEntries.add(volumeEntry);
            }

            assertEquals(2, retrievedEntries.size());
            assertEquals(versionstampedKeys[1], retrievedEntries.get(0).key());
            assertEquals(versionstampedKeys[2], retrievedEntries.get(1).key());
        }
    }

    @Test
    void shouldFilterWithFirstGreaterThan() throws IOException {
        ByteBuffer[] entries = getEntries(5);

        Versionstamp[] versionstampedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = result.getVersionstampedKeys();
        }

        // firstGreaterThan excludes the begin key
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterThan(versionstampedKeys[1]);
            VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(versionstampedKeys[4]);
            Iterable<VolumeEntry> iterable = volume.getRange(session, begin, end);

            List<VolumeEntry> retrievedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : iterable) {
                retrievedEntries.add(volumeEntry);
            }

            // Should get entries at index 2, 3 (excludes 1, excludes 4)
            assertEquals(2, retrievedEntries.size());
            assertEquals(versionstampedKeys[2], retrievedEntries.get(0).key());
            assertEquals(versionstampedKeys[3], retrievedEntries.get(1).key());
        }
    }

    @Test
    void shouldFilterWithBothFirstGreaterThan() throws IOException {
        ByteBuffer[] entries = getEntries(5);

        Versionstamp[] versionstampedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = result.getVersionstampedKeys();
        }

        // Both firstGreaterThan: excludes begin, includes up to but not including end
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterThan(versionstampedKeys[0]);
            VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterThan(versionstampedKeys[3]);
            Iterable<VolumeEntry> iterable = volume.getRange(session, begin, end);

            List<VolumeEntry> retrievedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : iterable) {
                retrievedEntries.add(volumeEntry);
            }

            // Should get entries at index 1, 2, 3 (excludes 0, includes up to 3)
            assertEquals(3, retrievedEntries.size());
            assertEquals(versionstampedKeys[1], retrievedEntries.get(0).key());
            assertEquals(versionstampedKeys[2], retrievedEntries.get(1).key());
            assertEquals(versionstampedKeys[3], retrievedEntries.get(2).key());
        }
    }

    @Test
    void shouldReturnCorrectMetadata() throws IOException {
        ByteBuffer[] entries = getEntries(3);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            result.getVersionstampedKeys();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = volume.getRange(session);

            for (VolumeEntry volumeEntry : iterable) {
                // Verify metadata is present
                assertNotNull(volumeEntry.metadata());
                assertTrue(volumeEntry.metadata().length > 0);

                // Verify metadata can be decoded
                EntryMetadata metadata = EntryMetadata.decode(volumeEntry.metadata());
                assertNotNull(metadata);

                // Verify metadata fields are valid
                assertTrue(metadata.segmentId() >= 0);
                assertTrue(metadata.length() > 0);
                assertTrue(metadata.position() >= 0);
            }
        }
    }

    @Test
    void shouldCombineLimitAndReverse() throws IOException {
        ByteBuffer[] entries = getEntries(5);

        Versionstamp[] versionstampedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = result.getVersionstampedKeys();
        }

        int limit = 2;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = volume.getRange(session, limit, true);

            List<VolumeEntry> retrievedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : iterable) {
                retrievedEntries.add(volumeEntry);
            }

            // Should return last 2 entries in reverse order
            assertEquals(limit, retrievedEntries.size());
            assertEquals(versionstampedKeys[4], retrievedEntries.get(0).key());
            assertEquals(versionstampedKeys[3], retrievedEntries.get(1).key());
        }
    }

    @Test
    void shouldCombineRangeAndLimit() throws IOException {
        ByteBuffer[] entries = getEntries(5);

        Versionstamp[] versionstampedKeys;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = result.getVersionstampedKeys();
        }

        // Range covers indices 1, 2, 3 but limit to 2
        int limit = 2;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(versionstampedKeys[1]);
            VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(versionstampedKeys[4]);
            Iterable<VolumeEntry> iterable = volume.getRange(session, begin, end, limit);

            List<VolumeEntry> retrievedEntries = new ArrayList<>();
            for (VolumeEntry volumeEntry : iterable) {
                retrievedEntries.add(volumeEntry);
            }

            // Should return first 2 entries within the range
            assertEquals(limit, retrievedEntries.size());
            assertEquals(versionstampedKeys[1], retrievedEntries.get(0).key());
            assertEquals(versionstampedKeys[2], retrievedEntries.get(1).key());
        }
    }
}
