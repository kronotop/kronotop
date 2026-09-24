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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class OnHeapVectorGraphIndexMetadataTest {
    private OnHeapVectorGraphIndexMetadata metadata;

    @BeforeEach
    void setUp() {
        metadata = new OnHeapVectorGraphIndexMetadata();
    }

    private EntryMetadata newEntryMetadata(long segmentId) {
        return new EntryMetadata(segmentId, new byte[8], 0L, 100L, 1L);
    }

    @Test
    void shouldPutAndFindNodeRef() {
        // Behavior: Putting an entry makes its ordinal and add versionstamp retrievable by ObjectId.
        ObjectId objectId = new ObjectId();
        Versionstamp versionstamp = TestUtil.generateVersionstamp(1);
        metadata.put(objectId, 42, versionstamp, 0, newEntryMetadata(1L));

        GraphNodeRef ref = metadata.findNodeRef(objectId);
        assertNotNull(ref);
        assertEquals(42, ref.ordinal());
        assertEquals(versionstamp, ref.versionstamp());
    }

    @Test
    void shouldPutAndFindDocumentLocation() {
        // Behavior: Putting an entry makes its DocumentLocation retrievable by ordinal.
        ObjectId objectId = new ObjectId();
        Versionstamp versionstamp = TestUtil.generateVersionstamp(1);
        EntryMetadata entryMetadata = newEntryMetadata(5L);
        metadata.put(objectId, 7, versionstamp, 0, entryMetadata);

        DocumentLocation location = metadata.findDocumentLocation(7);
        assertNotNull(location);
        assertEquals(objectId, location.objectId());
        assertEquals(0, location.shardId());
        assertEquals(entryMetadata, location.entryMetadata());
    }

    @Test
    void shouldQueueAndClearPendingDeletes() {
        // Behavior: removeMapping queues the removed ordinals for deletion, and clearPendingDeletes invokes the
        // consumer for each and clears the set.
        ObjectId first = new ObjectId();
        ObjectId second = new ObjectId();
        metadata.put(first, 0, makeVersionstamp(1, 0), 0, newEntryMetadata(1L));
        metadata.put(second, 3, makeVersionstamp(1, 0), 0, newEntryMetadata(2L));
        metadata.removeMapping(first, makeVersionstamp(2, 0));
        metadata.removeMapping(second, makeVersionstamp(2, 0));

        List<Integer> deleted = new ArrayList<>();
        metadata.clearPendingDeletes(deleted::add);

        assertEquals(2, deleted.size());
        assertTrue(deleted.contains(0));
        assertTrue(deleted.contains(3));

        // Calling again should yield nothing.
        List<Integer> secondPass = new ArrayList<>();
        metadata.clearPendingDeletes(secondPass::add);
        assertTrue(secondPass.isEmpty());
    }

    @Test
    void shouldReturnNullForUnknownObjectId() {
        // Behavior: findNodeRef returns null when the ObjectId is not present in the metadata.
        ObjectId unknownId = new ObjectId();
        assertNull(metadata.findNodeRef(unknownId));
    }

    @Test
    void shouldReturnMinusOneOnFirstPut() {
        // Behavior: The first put for an ObjectId has no stale and returns -1.
        assertEquals(-1, metadata.put(new ObjectId(), 3, makeVersionstamp(1, 0), 0, newEntryMetadata(1L)));
    }

    @Test
    void shouldOverwriteWithNewerVersionstampAndReturnPreviousOrdinal() {
        // Behavior: Putting the same ObjectId with a newer versionstamp replaces the mapping, even when the
        // new ordinal is lower. The previous ordinal is returned as the stale, its location is dropped,
        // and it is queued as a pending delete.
        ObjectId objectId = new ObjectId();
        metadata.put(objectId, 5, makeVersionstamp(1, 0), 0, newEntryMetadata(1L));
        int staleOrdinal = metadata.put(objectId, 0, makeVersionstamp(2, 0), 0, newEntryMetadata(2L));

        assertEquals(5, staleOrdinal);
        assertEquals(0, metadata.findNodeRef(objectId).ordinal());
        assertEquals(makeVersionstamp(2, 0), metadata.findNodeRef(objectId).versionstamp());
        assertNull(metadata.findDocumentLocation(5));
        assertNotNull(metadata.findDocumentLocation(0));
        List<Integer> pending = new ArrayList<>();
        metadata.clearPendingDeletes(pending::add);
        assertEquals(List.of(5), pending);
    }

    @Test
    void shouldOverwriteWithEqualVersionstamp() {
        // Behavior: Putting the same ObjectId with the same versionstamp replaces the mapping with the latest
        // value and returns the previous ordinal as the loser.
        ObjectId objectId = new ObjectId();
        metadata.put(objectId, 0, makeVersionstamp(1, 0), 0, newEntryMetadata(1L));
        int loser = metadata.put(objectId, 5, makeVersionstamp(1, 0), 0, newEntryMetadata(2L));

        assertEquals(0, loser);
        assertEquals(5, metadata.findNodeRef(objectId).ordinal());
        assertNull(metadata.findDocumentLocation(0));
    }

    @Test
    void shouldRejectStaleOverwriteAndReturnIncomingOrdinal() {
        // Behavior: Putting the same ObjectId with an older versionstamp keeps the existing mapping and
        // location, returns the incoming ordinal as the loser, and queues it as a pending delete.
        ObjectId objectId = new ObjectId();
        EntryMetadata original = newEntryMetadata(1L);
        metadata.put(objectId, 0, makeVersionstamp(5, 0), 0, original);
        int loser = metadata.put(objectId, 7, makeVersionstamp(2, 0), 0, newEntryMetadata(2L));

        assertEquals(7, loser);
        GraphNodeRef ref = metadata.findNodeRef(objectId);
        assertEquals(0, ref.ordinal());
        assertEquals(makeVersionstamp(5, 0), ref.versionstamp());
        assertNotNull(metadata.findDocumentLocation(0));
        assertNull(metadata.findDocumentLocation(7));
        List<Integer> pending = new ArrayList<>();
        metadata.clearPendingDeletes(pending::add);
        assertEquals(List.of(7), pending);
    }

    @Test
    void shouldRemoveMappingOnlyWithNewerDelete() {
        // Behavior: removeMapping with an older or equal versionstamp returns -1 and keeps the mapping.
        // A newer versionstamp removes the mapping and the location, queues the ordinal as a pending
        // delete, and returns the ordinal.
        ObjectId objectId = new ObjectId();
        metadata.put(objectId, 4, makeVersionstamp(3, 0), 0, newEntryMetadata(1L));

        assertEquals(-1, metadata.removeMapping(objectId, makeVersionstamp(2, 0)));
        assertEquals(-1, metadata.removeMapping(objectId, makeVersionstamp(3, 0)));
        assertNotNull(metadata.findNodeRef(objectId));
        assertNotNull(metadata.findDocumentLocation(4));

        assertEquals(4, metadata.removeMapping(objectId, makeVersionstamp(4, 0)));
        assertNull(metadata.findNodeRef(objectId));
        assertNull(metadata.findDocumentLocation(4));
        List<Integer> pending = new ArrayList<>();
        metadata.clearPendingDeletes(pending::add);
        assertEquals(List.of(4), pending);

        assertEquals(-1, metadata.removeMapping(new ObjectId(), makeVersionstamp(9, 0)));
    }

    private Versionstamp makeVersionstamp(int highByte, int userVersion) {
        byte[] trVersion = new byte[10];
        trVersion[0] = (byte) highByte;
        return Versionstamp.complete(trVersion, userVersion);
    }

    @Test
    void shouldTrackLatestVersionstamp() {
        // Behavior: advanceVersionstamp updates the latest versionstamp when called with increasing values.
        assertNull(metadata.getLatestVersionstamp());

        Versionstamp v1 = makeVersionstamp(1, 0);
        Versionstamp v2 = makeVersionstamp(2, 0);
        Versionstamp v3 = makeVersionstamp(3, 0);

        metadata.advanceVersionstamp(v1);
        assertEquals(v1, metadata.getLatestVersionstamp());

        metadata.advanceVersionstamp(v2);
        assertEquals(v2, metadata.getLatestVersionstamp());

        metadata.advanceVersionstamp(v3);
        assertEquals(v3, metadata.getLatestVersionstamp());
    }

    @Test
    void shouldNotRegressVersionstamp() {
        // Behavior: advanceVersionstamp ignores a versionstamp that is older than the current one.
        Versionstamp newer = makeVersionstamp(5, 0);
        Versionstamp older = makeVersionstamp(2, 0);

        metadata.advanceVersionstamp(newer);
        metadata.advanceVersionstamp(older);

        assertEquals(newer, metadata.getLatestVersionstamp());
    }

    @Test
    void shouldHandleConcurrentAdvances() throws InterruptedException {
        // Behavior: Under concurrent updates, the final versionstamp is always the maximum across all threads.
        int threadCount = 16;
        Versionstamp[] stamps = new Versionstamp[threadCount];
        for (int i = 0; i < threadCount; i++) {
            stamps[i] = makeVersionstamp(i + 1, 0);
        }
        Versionstamp expected = stamps[threadCount - 1];

        CountDownLatch startLatch = new CountDownLatch(1);
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int idx = i;
            threads[i] = Thread.ofVirtual().start(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                metadata.advanceVersionstamp(stamps[idx]);
            });
        }

        startLatch.countDown();
        for (Thread t : threads) {
            t.join();
        }

        assertEquals(expected, metadata.getLatestVersionstamp());
    }
}
