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

package com.kronotop.bucket.vector;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TestUtil;
import com.kronotop.bucket.pipeline.DocumentLocation;
import com.kronotop.volume.EntryMetadata;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OnDiskVectorGraphIndexMetadataTest {

    @TempDir
    Path tempDir;

    private EntryMetadata newEntryMetadata(long segmentId, long position, long length, long handle) {
        return new EntryMetadata(segmentId, new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, position, length, handle);
    }

    private Path writeAndOpen(OnHeapVectorGraphIndexMetadata heapMeta) throws IOException {
        Path path = tempDir.resolve("test.vmeta");
        OnDiskVectorGraphIndexMetadataWriter.write(path, heapMeta);
        return path;
    }

    @Test
    void shouldLookupNodeRefByObjectId() throws IOException {
        // Behavior: ObjectId lookup returns the ordinal and the add versionstamp of the node via binary search.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        List<ObjectId> ids = new ArrayList<>();
        List<Versionstamp> versionstamps = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ObjectId oid = new ObjectId();
            Versionstamp vs = TestUtil.generateVersionstamp(i);
            ids.add(oid);
            versionstamps.add(vs);
            heapMeta.put(oid, i, vs, 0, newEntryMetadata(i, i * 100L, 50L, i + 1000L));
        }

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            for (int i = 0; i < 10; i++) {
                GraphNodeRef ref = diskMeta.findNodeRef(ids.get(i));
                assertNotNull(ref);
                assertEquals(i, ref.ordinal());
                assertEquals(versionstamps.get(i), ref.versionstamp());
            }
        }
    }

    @Test
    void shouldLookupEntryMetadataByOrdinal() throws IOException {
        // Behavior: Ordinal-to-DocumentLocation lookup returns the correct metadata via direct offset.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        ObjectId oid = new ObjectId();
        EntryMetadata expected = newEntryMetadata(42L, 1024L, 256L, 9999L);
        heapMeta.put(oid, 3, TestUtil.zeroVersionstamp(), 0, expected);

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            DocumentLocation loc = diskMeta.findDocumentLocation(3);
            assertNotNull(loc);
            assertEquals(oid, loc.objectId());
            assertEquals(0, loc.shardId());
            EntryMetadata actual = loc.entryMetadata();
            assertEquals(expected.segmentId(), actual.segmentId());
            assertArrayEquals(expected.prefix(), actual.prefix());
            assertEquals(expected.position(), actual.position());
            assertEquals(expected.length(), actual.length());
            assertEquals(expected.handle(), actual.handle());
        }
    }

    @Test
    void shouldReturnNullForDeletedOrdinalGap() throws IOException {
        // Behavior: Ordinals that have no entry (gaps) return null.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        heapMeta.put(new ObjectId(), 0, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1L, 0L, 10L, 100L));
        heapMeta.put(new ObjectId(), 5, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2L, 0L, 10L, 200L));

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            assertNotNull(diskMeta.findDocumentLocation(0));
            assertNull(diskMeta.findDocumentLocation(1));
            assertNull(diskMeta.findDocumentLocation(2));
            assertNull(diskMeta.findDocumentLocation(3));
            assertNull(diskMeta.findDocumentLocation(4));
            assertNotNull(diskMeta.findDocumentLocation(5));
        }
    }

    @Test
    void shouldReturnNullForMissingObjectId() throws IOException {
        // Behavior: Looking up an ObjectId that was never inserted returns null.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        heapMeta.put(new ObjectId(), 0, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1L, 0L, 10L, 100L));

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            assertNull(diskMeta.findNodeRef(new ObjectId()));
        }
    }

    @Test
    void shouldReturnNullForOutOfRangeOrdinal() throws IOException {
        // Behavior: Ordinals outside the valid range return null.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        heapMeta.put(new ObjectId(), 0, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1L, 0L, 10L, 100L));

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            assertNull(diskMeta.findDocumentLocation(-1));
            assertNull(diskMeta.findDocumentLocation(1));
            assertNull(diskMeta.findDocumentLocation(100));
        }
    }

    @Test
    void shouldHandleSingleEntry() throws IOException {
        // Behavior: A single-entry metadata file works for both lookup directions.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        ObjectId oid = new ObjectId();
        EntryMetadata em = newEntryMetadata(7L, 512L, 128L, 42L);
        heapMeta.put(oid, 0, TestUtil.zeroVersionstamp(), 0, em);

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            assertEquals(0, diskMeta.findNodeRef(oid).ordinal());
            DocumentLocation loc = diskMeta.findDocumentLocation(0);
            assertNotNull(loc);
            assertEquals(oid, loc.objectId());
            assertEquals(em.segmentId(), loc.entryMetadata().segmentId());
            assertEquals(em.handle(), loc.entryMetadata().handle());
        }
    }

    @Test
    void shouldLookupFirstAndLastObjectId() throws IOException {
        // Behavior: Binary search correctly finds the first and last ObjectId in sorted order.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        List<ObjectId> ids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ObjectId oid = new ObjectId();
            ids.add(oid);
            heapMeta.put(oid, i, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(i, i * 10L, 5L, i + 1L));
        }

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            // Verify all IDs are findable (covers first and last in sorted order)
            for (int i = 0; i < 100; i++) {
                assertEquals(i, diskMeta.findNodeRef(ids.get(i)).ordinal());
            }
        }
    }

    @Test
    void shouldPersistAndReadLatestVersionstamp() throws IOException {
        // Behavior: latestVersionstamp set on heap metadata is persisted to disk and read back correctly.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        ObjectId oid = new ObjectId();
        heapMeta.put(oid, 0, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1L, 0L, 10L, 100L));

        Versionstamp expected = TestUtil.generateVersionstamp(42);
        heapMeta.advanceVersionstamp(expected);

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            Versionstamp actual = diskMeta.getLatestVersionstamp();
            assertNotNull(actual);
            assertArrayEquals(expected.getBytes(), actual.getBytes());
        }
    }

    @Test
    void shouldMarkDeletedAndReturnNullFromFindDocumentLocation() throws IOException {
        // Behavior: markDeleted zeroes the ALIVE byte so findEntryMetadata returns null for that ordinal only.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        for (int i = 0; i < 3; i++) {
            heapMeta.put(new ObjectId(), i, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(i, i * 100L, 50L, i + 1000L));
        }

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            assertNotNull(diskMeta.findDocumentLocation(1));
            diskMeta.markDeleted(1);
            assertNull(diskMeta.findDocumentLocation(1));
            assertNotNull(diskMeta.findDocumentLocation(0));
            assertNotNull(diskMeta.findDocumentLocation(2));
        }
    }

    @Test
    void shouldPersistMarkDeletedAfterFlush() throws IOException {
        // Behavior: After markDeleted + flush, the deletion survives close/reopen from the same file.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        for (int i = 0; i < 2; i++) {
            heapMeta.put(new ObjectId(), i, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(i, i * 100L, 50L, i + 1000L));
        }

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            diskMeta.markDeleted(0);
            diskMeta.flush();
        }

        try (OnDiskVectorGraphIndexMetadata reopened = new OnDiskVectorGraphIndexMetadata(path)) {
            assertNull(reopened.findDocumentLocation(0));
            assertNotNull(reopened.findDocumentLocation(1));
        }
    }

    @Test
    void shouldFlushNoOpWhenNoPendingChanges() throws IOException {
        // Behavior: Calling flush with no prior markDeleted does not throw and leaves data intact.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        heapMeta.put(new ObjectId(), 0, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1L, 0L, 10L, 100L));

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            diskMeta.flush();
            assertNotNull(diskMeta.findDocumentLocation(0));
        }
    }

    @Test
    void shouldReadDeletedCountAsZeroForNewFile() throws IOException {
        // Behavior: A freshly written metadata file has deletedCount of zero.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        for (int i = 0; i < 3; i++) {
            heapMeta.put(new ObjectId(), i, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(i, i * 100L, 50L, i + 1000L));
        }

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            assertEquals(0, diskMeta.getDeletedCount());
        }
    }

    @Test
    void shouldIncrementDeletedCountOnMarkDeleted() throws IOException {
        // Behavior: Each markDeleted call increments deletedCount by one.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        for (int i = 0; i < 3; i++) {
            heapMeta.put(new ObjectId(), i, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(i, i * 100L, 50L, i + 1000L));
        }

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            diskMeta.markDeleted(0);
            assertEquals(1, diskMeta.getDeletedCount());
            diskMeta.markDeleted(2);
            assertEquals(2, diskMeta.getDeletedCount());
        }
    }

    @Test
    void shouldPersistDeletedCountAfterFlush() throws IOException {
        // Behavior: deletedCount survives flush + close/reopen, and undeleted entries remain accessible.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        for (int i = 0; i < 3; i++) {
            heapMeta.put(new ObjectId(), i, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(i, i * 100L, 50L, i + 1000L));
        }

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            diskMeta.markDeleted(0);
            diskMeta.markDeleted(2);
            diskMeta.flush();
        }

        try (OnDiskVectorGraphIndexMetadata reopened = new OnDiskVectorGraphIndexMetadata(path)) {
            assertEquals(2, reopened.getDeletedCount());
            assertNotNull(reopened.findDocumentLocation(1));
        }
    }

    @Test
    void shouldNotIncrementDeletedCountWhenMarkDeletedCalledTwice() throws IOException {
        // Behavior: markDeleted on an already deleted ordinal does not change deletedCount, so a retried delete counts once.
        OnHeapVectorGraphIndexMetadata heapMeta = new OnHeapVectorGraphIndexMetadata();
        for (int i = 0; i < 3; i++) {
            heapMeta.put(new ObjectId(), i, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(i, i * 100L, 50L, i + 1000L));
        }

        Path path = writeAndOpen(heapMeta);
        try (OnDiskVectorGraphIndexMetadata diskMeta = new OnDiskVectorGraphIndexMetadata(path)) {
            diskMeta.markDeleted(0);
            diskMeta.markDeleted(0);
            assertEquals(1, diskMeta.getDeletedCount());
            diskMeta.flush();
        }

        try (OnDiskVectorGraphIndexMetadata reopened = new OnDiskVectorGraphIndexMetadata(path)) {
            assertEquals(1, reopened.getDeletedCount());
        }
    }

}
