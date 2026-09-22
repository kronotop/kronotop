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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.*;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeTestUtil;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class ReplayFailedOpsLogTest extends BaseStandaloneInstanceTest {
    private static final String SELECTOR = "embedding";
    private static final int DIMENSIONS = 3;
    private static final int SHARD_ID = 1;
    private static final float[] TEST_VECTOR_1 = {0.1f, 0.2f, 0.3f};
    private static final float[] TEST_VECTOR_2 = {0.4f, 0.5f, 0.6f};

    private ExecutorService executor;

    @BeforeEach
    void setUpExecutor() {
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @AfterEach
    void tearDownExecutor() {
        executor.close();
    }

    private static Versionstamp versionstamp(int txVersion) {
        byte[] trVersion = new byte[10];
        ByteBuffer.wrap(trVersion).putInt(6, txVersion);
        return Versionstamp.complete(trVersion, 0);
    }

    private EntryMetadata newEntryMetadata() {
        return VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test");
    }

    private byte[] encodedIndexEntry() {
        return new IndexEntry(SHARD_ID, newEntryMetadata().encode()).encode();
    }

    private VectorIndex createVectorIndex() {
        createBucket(TEST_BUCKET);
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        VectorIndexDefinition definition = VectorIndexDefinition.create(name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, IndexStatus.WAITING);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, getBucketMetadata(TEST_BUCKET), definition);
            tr.commit().join();
        }
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        return metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
    }

    private List<KeyValue> readFailedOpLog(VectorIndex vectorIndex) {
        byte[] prefix = vectorIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.FAILED_OP_LOG.getValue()));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(prefix, ByteArrayUtil.strinc(prefix)).asList().join();
        }
    }

    @Test
    void shouldReplayFailedInserts() throws IOException {
        // Behavior: INSERT entries in the FAILED_OP_LOG are added to the on-heap index, and the latest versionstamp advances.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        Versionstamp vs1 = versionstamp(1);
        Versionstamp vs2 = versionstamp(2);
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setFailedOpLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT, vs1,
                    oid1.toByteArray(), encoded, TEST_VECTOR_1);
            VectorIndexMaintainer.setFailedOpLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT, vs2,
                    oid2.toByteArray(), encoded, TEST_VECTOR_2);
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);

        FailedOps failedOps = ReplayFailedOpsLog.replay(
                context.getFoundationDB(), group, onHeap, vectorIndex.subspace(), executor);

        assertTrue(failedOps.adds().isEmpty());
        assertTrue(failedOps.deletes().isEmpty());
        assertEquals(2, onHeap.size());
        assertTrue(onHeap.getMetadata().findOrdinal(oid1) >= 0);
        assertTrue(onHeap.getMetadata().findOrdinal(oid2) > 0);
        assertEquals(vs2, onHeap.getLatestVersionstamp());
        onHeap.close();
    }

    @Test
    void shouldReplayFailedDeleteOnHeap() throws IOException {
        // Behavior: A DELETE entry in the FAILED_OP_LOG removes the node mapping from the on-heap index.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid = new ObjectId();
        Versionstamp vs = versionstamp(1);

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        onHeap.addGraphNode(oid, SHARD_ID, newEntryMetadata(), TEST_VECTOR_1, executor).join();
        assertTrue(onHeap.getMetadata().findOrdinal(oid) >= 0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.deleteFailedOpLog(tr, vectorIndex.subspace(), vs, oid.toByteArray());
            tr.commit().join();
        }

        FailedOps failedOps = ReplayFailedOpsLog.replay(
                context.getFoundationDB(), group, onHeap, vectorIndex.subspace(), executor);

        assertTrue(failedOps.adds().isEmpty());
        assertTrue(failedOps.deletes().isEmpty());
        assertEquals(-1, onHeap.getMetadata().findOrdinal(oid));
        onHeap.close();
    }

    @Test
    void shouldReturnEmptyFailedOpsWhenLogIsEmpty() throws IOException {
        // Behavior: An empty FAILED_OP_LOG leaves the on-heap index unchanged and returns empty lists.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);

        FailedOps failedOps = ReplayFailedOpsLog.replay(
                context.getFoundationDB(), group, onHeap, vectorIndex.subspace(), executor);

        assertTrue(failedOps.adds().isEmpty());
        assertTrue(failedOps.deletes().isEmpty());
        assertEquals(0, onHeap.size());
        assertNull(onHeap.getLatestVersionstamp());
        onHeap.close();
    }

    @Test
    void shouldKeepLogEntriesAfterReplay() throws IOException {
        // Behavior: Replay does not remove entries from the FAILED_OP_LOG.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid = new ObjectId();
        Versionstamp vs = versionstamp(1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setFailedOpLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT, vs,
                    oid.toByteArray(), encodedIndexEntry(), TEST_VECTOR_1);
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        ReplayFailedOpsLog.replay(context.getFoundationDB(), group, onHeap, vectorIndex.subspace(), executor);

        assertEquals(1, readFailedOpLog(vectorIndex).size());
        assertEquals(1, onHeap.size());
        onHeap.close();
    }

    @Test
    void shouldReplayLogLargerThanOnePage() throws IOException {
        // Behavior: A FAILED_OP_LOG larger than one page is replayed completely across several transactions.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        int total = ReplayFailedOpsLog.PAGE_SIZE + 1;
        List<ObjectId> oids = new ArrayList<>(total);
        byte[] encoded = encodedIndexEntry();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 1; i <= total; i++) {
                ObjectId oid = new ObjectId();
                oids.add(oid);
                VectorIndexMaintainer.setFailedOpLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                        versionstamp(i), oid.toByteArray(), encoded, TEST_VECTOR_1);
            }
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        FailedOps failedOps = ReplayFailedOpsLog.replay(
                context.getFoundationDB(), group, onHeap, vectorIndex.subspace(), executor);

        assertTrue(failedOps.adds().isEmpty());
        assertEquals(total, onHeap.size());
        for (ObjectId oid : oids) {
            assertTrue(onHeap.getMetadata().findOrdinal(oid) >= 0);
        }
        assertEquals(versionstamp(total), onHeap.getLatestVersionstamp());
        onHeap.close();
    }
}
