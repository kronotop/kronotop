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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.BaseVolumeIntegrationTest;
import com.kronotop.volume.OperationKind;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class SegmentLogTest extends BaseVolumeIntegrationTest {

    @Test
    public void test_append() throws IOException {
        SegmentConfig segmentConfig = new SegmentConfig(1, volume.getConfig().dataDir(), 0xfffff);
        Segment segment = new Segment(segmentConfig);
        SegmentLog segmentLog = new SegmentLog(segment.getName(), volume.getConfig().subspace());

        try (Transaction tr = database.createTransaction()) {
            SegmentLogValue entry = new SegmentLogValue(OperationKind.APPEND, prefix.asLong(), 0, 100);
            assertDoesNotThrow(() -> segmentLog.append(tr, 0, entry));
            tr.commit().join();
        }
    }

    @Test
    public void test_SegmentLogIterable() throws IOException {
        SegmentConfig segmentConfig = new SegmentConfig(1, volume.getConfig().dataDir(), 0xfffff);
        Segment segment = new Segment(segmentConfig);
        SegmentLog segmentLog = new SegmentLog(segment.getName(), volume.getConfig().subspace());
        List<Versionstamp> keys = new ArrayList<>();
        List<SegmentLogValue> values = new ArrayList<>();

        try (Transaction tr = database.createTransaction()) {
            long start = 0;
            long length = 100;
            for (int userVersion = 0; userVersion < 10; userVersion++) {
                SegmentLogValue value = new SegmentLogValue(OperationKind.APPEND, prefix.asLong(), start, length);
                values.add(value);

                int finalUserVersion = userVersion;
                assertDoesNotThrow(() -> segmentLog.append(tr, finalUserVersion, value));
                start += length;
            }
            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.commit().join();

            byte[] trVersion = future.join();
            for (int userVersion = 0; userVersion < 10; userVersion++) {
                keys.add(Versionstamp.complete(trVersion, userVersion));
            }
        }

        List<SegmentLogEntry> entries = new ArrayList<>();
        try (Transaction tr = database.createTransaction()) {
            SegmentLogIterable iterable = new SegmentLogIterable(tr, volume.getConfig().subspace(), segment.getName());
            for (SegmentLogEntry segmentLogEntry : iterable) {
                entries.add(segmentLogEntry);
            }
        }
        assertEquals(10, entries.size());

        for (int i = 0; i < entries.size(); i++) {
            SegmentLogEntry entry = entries.get(i);
            Versionstamp key = keys.get(i);
            SegmentLogValue value = values.get(i);
            assertEquals(key, entry.key());
            assertEquals(value, entry.value());
            assertTrue(entry.timestamp() > 0);
        }
    }

    @Test
    public void test_SegmentLogIterable_range() throws IOException {
        SegmentConfig segmentConfig = new SegmentConfig(1, volume.getConfig().dataDir(), 0xfffff);
        Segment segment = new Segment(segmentConfig);
        SegmentLog segmentLog = new SegmentLog(segment.getName(), volume.getConfig().subspace());
        List<Versionstamp> keys = new ArrayList<>();
        List<SegmentLogValue> values = new ArrayList<>();

        try (Transaction tr = database.createTransaction()) {
            long start = 0;
            long length = 100;
            for (int userVersion = 0; userVersion < 10; userVersion++) {
                SegmentLogValue value = new SegmentLogValue(OperationKind.APPEND, prefix.asLong(), start, length);
                values.add(value);

                segmentLog.append(tr, userVersion, value);
                start += length;
            }
            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.commit().join();

            byte[] trVersion = future.join();
            for (int userVersion = 0; userVersion < 10; userVersion++) {
                keys.add(Versionstamp.complete(trVersion, userVersion));
            }
        }

        // [begin, end)
        VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(keys.get(3));
        VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(keys.get(7));

        List<SegmentLogEntry> entries = new ArrayList<>();
        try (Transaction tr = database.createTransaction()) {
            SegmentLogIterable iterable = new SegmentLogIterable(tr, volume.getConfig().subspace(), segment.getName(), begin, end);
            for (SegmentLogEntry segmentLogEntry : iterable) {
                entries.add(segmentLogEntry);
            }
        }

        assertEquals(4, entries.size());

        for (int i = 0; i < entries.size(); i++) {
            SegmentLogEntry entry = entries.get(i);
            Versionstamp key = keys.get(i + 3);
            assertEquals(key, entry.key());
            SegmentLogValue value = values.get(i + 3);
            assertEquals(value, entry.value());
        }
    }

    @Test
    public void test_getCardinality() throws IOException {
        SegmentConfig segmentConfig = new SegmentConfig(1, volume.getConfig().dataDir(), 0xfffff);
        Segment segment = new Segment(segmentConfig);
        SegmentLog segmentLog = new SegmentLog(segment.getName(), volume.getConfig().subspace());

        int total = 100;
        try (Transaction tr = database.createTransaction()) {
            for (int userVersion = 0; userVersion < total; userVersion++) {
                SegmentLogValue entry = new SegmentLogValue(OperationKind.APPEND, prefix.asLong(),0, 100);
                int finalUserVersion = userVersion;
                assertDoesNotThrow(() -> segmentLog.append(tr, finalUserVersion, entry));
            }
            tr.commit().join();
        }

        try (Transaction tr = database.createTransaction()) {
            assertEquals(total, segmentLog.getCardinality(tr));
        }
    }
}