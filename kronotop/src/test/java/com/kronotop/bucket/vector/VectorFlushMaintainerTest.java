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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.index.IndexEntry;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.MutationLogKind;
import com.kronotop.bucket.index.VectorIndexMaintainer;
import com.kronotop.volume.VolumeTestUtil;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorFlushMaintainerTest extends BaseVectorMaintainerTest {

    private VectorFlushTask newTask(ObjectId generationId) {
        VectorFlushKey key = new VectorFlushKey(metadata.namespace(), metadata.name(), vectorIndex.definition().id(), generationId);
        return new VectorFlushTask(key, 0);
    }

    private List<KeyValue> mutationLogEntries(ObjectId generationId) {
        byte[] prefix = vectorIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.VECTOR_MUTATION_LOG.getValue(), generationId.toByteArray()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        try (Transaction tr = context.getTransactionManager().create()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    @Test
    void shouldRescheduleWhenGenerationStillHasData() {
        // Behavior: When the on-heap index of a generation received a mutation inside the idle window,
        // the maintainer does not flush it and puts the task back into the queue with a later due time.
        ObjectId generationId = new ObjectId();
        addOneVector(generationId);
        service.getVectorFlushQueue().clear();

        service.getVectorFlushQueue().add(newTask(generationId));

        await().atMost(Duration.ofSeconds(5)).until(() ->
                service.getVectorFlushQueue().stream().anyMatch(task ->
                        task.key().generationId().equals(generationId) && task.dueNanos() > 0)
        );
        assertFalse(group.hasNoOnHeapIndexes(generationId));
        assertTrue(group.getOnDiskIndexes().isEmpty());
    }

    @Test
    void shouldFlushIdleIndexAndTruncateMutationLog() {
        // Behavior: When the on-heap index of a generation stayed idle past the idle window,
        // the maintainer writes it to disk, records (GRAPH_FLUSHED, id) in FDB and removes the
        // mutation log entries of that generation.
        ObjectId generationId = new ObjectId();
        OnHeapVectorGraphIndex onHeap = addOneVector(generationId);
        byte[] encodedEntry = new IndexEntry(1, VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test").encode()).encode();
        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(),
                    generationId.toByteArray(),
                    MutationLogKind.INSERT,
                    1,
                    new ObjectId().toByteArray(),
                    encodedEntry,
                    new float[]{1.0f, 0.0f, 0.0f},
                    0
            );
            tr.commit().join();
        }
        assertEquals(1, mutationLogEntries(generationId).size());
        service.getVectorFlushQueue().clear();

        service.getVectorFlushQueue().add(newTask(generationId));

        await().atMost(Duration.ofSeconds(10)).until(() -> group.hasNoOnHeapIndexes(generationId));
        assertEquals(1, group.getOnDiskIndexes().size());
        assertTrue(hasKey(IndexSubspaceMagic.GRAPH_FLUSHED.getValue(), onHeap.getId()));
        await().atMost(Duration.ofSeconds(5)).until(() -> mutationLogEntries(generationId).isEmpty());
    }
}
