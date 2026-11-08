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

import com.kronotop.internal.DirectoryUtil;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorIndexSyncBacklogTest extends BaseVectorMaintainerTest {

    private int enqueue() {
        return VectorIndexSyncBacklog.enqueue(context, metadata, vectorIndex, service.getBucketDataDir());
    }

    @Test
    void shouldEnqueueFlushedButUnsyncedIndex() throws Exception {
        // Behavior: An on-heap index with one vector is flushed, so FDB holds (GRAPH_FLUSHED, id), the
        // .complete marker is on disk, there is no (GRAPH_SYNCED, id) key and the sync queue is empty.
        // The backlog scan queues one sync task. The sync maintainer runs it and the (GRAPH_SYNCED, id)
        // key appears. The sync failure counter stays at zero.
        try (OnHeapVectorGraphIndex onHeap = flushOneVector()) {
            ObjectId indexId = onHeap.getId();
            assertFalse(isSynced(indexId));

            assertEquals(1, enqueue());

            await().atMost(Duration.ofSeconds(10)).until(() -> isSynced(indexId));
            assertEquals(0, service.getMetrics().getVectorSyncFailures());
        }
    }

    @Test
    void shouldSkipSyncedIndex() throws Exception {
        // Behavior: An index is flushed and its sync task has run, so both (GRAPH_FLUSHED, id) and
        // (GRAPH_SYNCED, id) exist. The backlog scan queues nothing and the sync queue stays empty.
        try (OnHeapVectorGraphIndex onHeap = flushOneVector()) {
            ObjectId indexId = onHeap.getId();
            service.getVectorSyncQueue().add(newSyncTask(indexId));
            await().atMost(Duration.ofSeconds(10)).until(() -> isSynced(indexId));

            assertEquals(0, enqueue());

            assertTrue(service.getVectorSyncQueue().isEmpty());
        }
    }

    @Test
    void shouldSkipIndexWhoseFilesAreMissing() throws Exception {
        // Behavior: An index is flushed but not synced, then its index directory is deleted from the local
        // disk. The backlog scan queues nothing, the sync queue stays empty, (GRAPH_SYNCED, id) stays
        // absent and the sync failure counter is incremented by one.
        try (OnHeapVectorGraphIndex onHeap = flushOneVector()) {
            ObjectId indexId = onHeap.getId();
            DirectoryUtil.deleteRecursively(indexDir());

            assertEquals(0, enqueue());

            assertTrue(service.getVectorSyncQueue().isEmpty());
            assertFalse(isSynced(indexId));
            assertEquals(1, service.getMetrics().getVectorSyncFailures());
        }
    }

    @Test
    void shouldSyncBacklogOnBootstrap() throws Exception {
        // Behavior: An index is flushed but not synced and its files stay on disk with the .complete marker.
        // The group is removed from the registry. A new group for the same index takes the slow path because
        // a .complete file exists. After bootstrap the group holds one on-disk index and the sync maintainer
        // writes the graph to Volume, so (GRAPH_SYNCED, id) appears without any sync task added by the test.
        ObjectId indexId;
        try (OnHeapVectorGraphIndex onHeap = flushOneVector()) {
            indexId = onHeap.getId();
        }
        service.getVectorGraphRegistry().remove(metadata.namespace(), metadata.name(), vectorIndex.definition().id());

        group = service.getVectorGraphRegistry().computeIfAbsent(
                metadata, vectorIndex, () -> service.bootstrapVectorGroup(metadata, vectorIndex));
        group.awaitReady();

        assertEquals(1, group.getOnDiskIndexes().size());
        await().atMost(Duration.ofSeconds(10)).until(() -> isSynced(indexId));
    }
}
