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

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.index.VectorIndexMaintainer;
import com.kronotop.internal.DirectoryUtil;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorIndexVolumeRestoreTest extends BaseVectorMaintainerTest {
    private static final int READ_CHUNKS_PER_TRANSACTION = 2;

    private ObjectId flushAndSyncOneVector() throws Exception {
        try (OnHeapVectorGraphIndex onHeap = flushOneVector()) {
            ObjectId indexId = onHeap.getId();
            service.getVectorSyncQueue().add(newSyncTask(indexId));
            await().atMost(Duration.ofSeconds(10)).until(() -> isSynced(indexId));
            return indexId;
        }
    }

    private void restore() throws Exception {
        VectorIndexVolumeRestore.restore(context, metadata, vectorIndex, service.getBucketDataDir(), READ_CHUNKS_PER_TRANSACTION);
    }

    private Path file(ObjectId indexId, String extension) {
        return indexDir().resolve(indexId.toHexString() + extension);
    }

    @Test
    void shouldRestoreSyncedIndexFromVolume() throws Exception {
        // Behavior: An on-heap index with one vector is flushed and synced to Volume, then its index
        // directory is deleted from the local disk. Restore reads the chunks back from Volume. The .index
        // and .vmeta files are created with the same bytes as before, the .complete marker is created, and
        // there is no .pqv file because PQ was not trained. The sync failure counter stays at zero.
        ObjectId indexId = flushAndSyncOneVector();
        byte[] index = Files.readAllBytes(file(indexId, ".index"));
        byte[] vmeta = Files.readAllBytes(file(indexId, ".vmeta"));
        DirectoryUtil.deleteRecursively(indexDir());
        assertFalse(Files.exists(indexDir()));

        restore();

        assertArrayEquals(index, Files.readAllBytes(file(indexId, ".index")));
        assertArrayEquals(vmeta, Files.readAllBytes(file(indexId, ".vmeta")));
        assertTrue(Files.exists(file(indexId, ".complete")));
        assertFalse(Files.exists(file(indexId, ".pqv")));
        assertEquals(0, service.getMetrics().getVectorSyncFailures());
    }

    @Test
    void shouldSkipIndexThatIsCompleteOnDisk() throws Exception {
        // Behavior: An index is flushed and synced, and its files stay on the local disk with the .complete
        // marker. Restore does not touch them. The modification times of the .index, .vmeta and .complete
        // files are the same after the call.
        ObjectId indexId = flushAndSyncOneVector();
        List<Path> files = List.of(file(indexId, ".index"), file(indexId, ".vmeta"), file(indexId, ".complete"));
        FileTime marker = FileTime.fromMillis(1_000_000_000_000L);
        for (Path path : files) {
            Files.setLastModifiedTime(path, marker);
        }

        restore();

        for (Path path : files) {
            assertEquals(marker, Files.getLastModifiedTime(path), path.toString());
        }
    }

    @Test
    void shouldNotCreateCompleteMarkerWhenChunksAreMissing() throws Exception {
        // Behavior: A (GRAPH_SYNCED, id) key exists but the Volume prefix of that id holds no chunks.
        // Restore does not throw, writes no .index, .vmeta or .complete file for that id, and increments
        // the sync failure counter by one.
        ObjectId indexId = new ObjectId();
        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.markGraphSynced(tr, vectorIndex.subspace(), indexId);
            tr.commit().join();
        }

        restore();

        assertFalse(Files.exists(file(indexId, ".index")));
        assertFalse(Files.exists(file(indexId, ".vmeta")));
        assertFalse(Files.exists(file(indexId, ".complete")));
        assertEquals(1, service.getMetrics().getVectorSyncFailures());
    }

    @Test
    void shouldBootstrapGroupFromVolumeAfterDiskLoss() throws Exception {
        // Behavior: An index with one vector is flushed and synced. Then the group is removed from the
        // registry and the index directory is deleted, so the local disk is empty and so is the mutation log.
        // A new group for the same index takes the slow path because a GRAPH_SYNCED key exists. After
        // bootstrap the group holds one on-disk index, and a search for the stored vector returns one hit.
        ObjectId indexId = flushAndSyncOneVector();
        service.getVectorGraphRegistry().remove(metadata.namespace(), metadata.name(), vectorIndex.definition().id());
        DirectoryUtil.deleteRecursively(indexDir());

        group = service.getVectorGraphRegistry().computeIfAbsent(
                metadata, vectorIndex, () -> service.bootstrapVectorGroup(metadata, vectorIndex));
        group.awaitReady();

        assertTrue(Files.exists(file(indexId, ".complete")));
        assertEquals(1, group.getOnDiskIndexes().size());
        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(1, results.size());
    }
}
