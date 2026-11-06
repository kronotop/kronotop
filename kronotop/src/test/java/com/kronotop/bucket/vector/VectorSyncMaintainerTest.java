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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.*;
import com.kronotop.volume.blob.BlobChunkReader;
import com.kronotop.volume.blob.DirectoryBlobChunkSink;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorSyncMaintainerTest extends BaseVectorMaintainerTest {

    private List<VolumeEntry> entriesUnder(Prefix prefix) {
        try (Transaction tr = context.getTransactionManager().create()) {
            List<VolumeEntry> entries = new ArrayList<>();
            volume().getRange(new VolumeSession(tr, prefix)).forEach(entries::add);
            return entries;
        }
    }

    private void assertRestoredFilesMatch(ObjectId indexId, Path out) throws Exception {
        Prefix prefix = prefixOf(indexId);

        try (DirectoryBlobChunkSink sink = new DirectoryBlobChunkSink(out)) {
            BlobChunkReader reader = new BlobChunkReader(volume(), prefix);
            Versionstamp cursor = null;
            do {
                try (Transaction tr = context.getTransactionManager().create()) {
                    cursor = reader.read(tr, cursor, 2, sink);
                }
            } while (cursor != null);
        }

        for (String extension : new String[]{".index", ".vmeta"}) {
            String name = indexId.toHexString() + extension;
            assertArrayEquals(Files.readAllBytes(indexDir().resolve(name)), Files.readAllBytes(out.resolve(name)), name);
        }
        assertFalse(Files.exists(out.resolve(indexId.toHexString() + ".pqv")));
    }

    @Test
    void shouldSyncFlushedIndexToVolume(@TempDir Path out) throws Exception {
        // Behavior: An on-heap index with one vector is flushed and the sync task for its id runs. The
        // maintainer writes the .index and .vmeta files to the owner shard Volume under the prefix built
        // from the id. There is no .pqv file because PQ is not trained with one vector. Reading the chunks
        // back gives files equal to the ones on disk. The (GRAPH_SYNCED, id) key exists in FDB. The prefix
        // registry holds a VECTOR_INDEX entry that is not stale and whose pointer is the id bytes. The sync
        // failure counter stays at zero.
        try (OnHeapVectorGraphIndex onHeap = flushOneVector()) {
            ObjectId indexId = onHeap.getId();

            service.getVectorSyncQueue().add(newSyncTask(indexId));

            await().atMost(Duration.ofSeconds(10)).until(() -> isSynced(indexId));
            assertRestoredFilesMatch(indexId, out);
            try (Transaction tr = context.getTransactionManager().create()) {
                PrefixInspection inspection = PrefixUtil.inspectPrefix(context, tr, prefixOf(indexId));
                assertFalse(inspection.isStale());
                assertEquals(EntryKind.VECTOR_INDEX, inspection.entry().kind());
                assertArrayEquals(indexId.toByteArray(), inspection.entry().pointer());
            }
            assertEquals(0, service.getMetrics().getVectorSyncFailures());
        }
    }

    @Test
    void shouldSkipTaskWhenGroupIsGone() {
        // Behavior: A sync task points to a vector index group that is not in the registry, as after an
        // index drop. The maintainer consumes the task and writes nothing. The prefix has no Volume entry,
        // the (GRAPH_SYNCED, id) key is absent, and the sync failure counter stays at zero.
        ObjectId indexId = new ObjectId();
        VectorSyncTask task = new VectorSyncTask(metadata.namespace(), metadata.name(), Long.MAX_VALUE, indexId);

        service.getVectorSyncQueue().add(task);

        await().atMost(Duration.ofSeconds(5)).until(() -> service.getVectorSyncQueue().isEmpty());
        assertTrue(entriesUnder(prefixOf(indexId)).isEmpty());
        assertFalse(isSynced(indexId));
        assertEquals(0, service.getMetrics().getVectorSyncFailures());
    }

    @Test
    void shouldClearPartialChunksBeforeSync(@TempDir Path out) throws Exception {
        // Behavior: An entry that is not a valid chunk sits under the prefix of a flushed index before the
        // sync, like a leftover from a sync that stopped halfway. The maintainer clears that entry first
        // and then writes the chunks. After the sync, reading the chunks back gives files equal to the ones
        // on disk.
        try (OnHeapVectorGraphIndex onHeap = flushOneVector()) {
            ObjectId indexId = onHeap.getId();
            try (Transaction tr = context.getTransactionManager().create()) {
                volume().append(new VolumeSession(tr, prefixOf(indexId)), ByteBuffer.wrap(new byte[]{1, 2, 3}));
                tr.commit().join();
            }
            assertEquals(1, entriesUnder(prefixOf(indexId)).size());

            service.getVectorSyncQueue().add(newSyncTask(indexId));

            await().atMost(Duration.ofSeconds(10)).until(() -> isSynced(indexId));
            assertRestoredFilesMatch(indexId, out);
        }
    }
}
