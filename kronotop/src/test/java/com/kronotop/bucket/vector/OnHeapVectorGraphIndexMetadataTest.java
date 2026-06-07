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
    void shouldPutAndFindOrdinal() {
        // Behavior: Putting an entry makes its ordinal retrievable by ObjectId.
        ObjectId objectId = new ObjectId();
        metadata.put(objectId, 42, 0, newEntryMetadata(1L));

        assertEquals(42, metadata.findOrdinal(objectId));
    }

    @Test
    void shouldPutAndFindDocumentLocation() {
        // Behavior: Putting an entry makes its DocumentLocation retrievable by ordinal.
        ObjectId objectId = new ObjectId();
        Versionstamp versionstamp = TestUtil.generateVersionstamp(1);
        EntryMetadata entryMetadata = newEntryMetadata(5L);
        metadata.put(objectId, 7, 0, entryMetadata);

        DocumentLocation location = metadata.findDocumentLocation(7);
        assertNotNull(location);
        assertEquals(objectId, location.objectId());
        assertEquals(0, location.shardId());
        assertEquals(entryMetadata, location.entryMetadata());
    }

    @Test
    void shouldAddAndClearPendingDeletes() {
        // Behavior: addPendingDelete records nodes for deletion, and clearPendingDeletes invokes the consumer for each and clears the set.
        metadata.addPendingDelete(0);
        metadata.addPendingDelete(3);

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
    void shouldReturnMinusOneForUnknownObjectId() {
        // Behavior: findOrdinal returns -1 when the ObjectId is not present in the metadata.
        ObjectId unknownId = new ObjectId();
        assertEquals(-1, metadata.findOrdinal(unknownId));
    }

    @Test
    void shouldOverwriteOnDuplicateObjectId() {
        // Behavior: Putting the same ObjectId twice overwrites the ordinal mapping with the latest value.
        ObjectId objectId = new ObjectId();
        metadata.put(objectId, 0, 0, newEntryMetadata(1L));
        metadata.put(objectId, 5, 0, newEntryMetadata(2L));

        assertEquals(5, metadata.findOrdinal(objectId));
    }

    @Test
    void shouldRejectStaleOverwriteWithLowerOrdinal() {
        // Behavior: Putting the same ObjectId with a lower ordinal is a no-op; the higher ordinal is retained.
        ObjectId objectId = new ObjectId();
        EntryMetadata original = newEntryMetadata(1L);
        metadata.put(objectId, 5, 0, original);
        metadata.put(objectId, 0, 0, newEntryMetadata(2L));

        assertEquals(5, metadata.findOrdinal(objectId));
        assertNotNull(metadata.findDocumentLocation(5));
        assertNull(metadata.findDocumentLocation(0));
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
