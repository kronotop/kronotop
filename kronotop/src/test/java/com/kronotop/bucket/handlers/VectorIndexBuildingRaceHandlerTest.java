/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.DistanceFunction;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.VectorIndexDefinition;
import com.kronotop.bucket.index.VectorIndexUtil;
import com.kronotop.bucket.index.VectorIndexValue;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ArrayRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduces the vector-index variant of the index-building race. Adding a vector index with
 * {@code BUCKET.INDEX} to an already populated, already cached bucket creates the index in
 * {@code WAITING} status and schedules a boundary task that later flips it to {@code BUILDING}
 * then {@code READY}. Documents inserted during that {@code WAITING} window can be permanently
 * absent from the vector index.
 *
 * <h2>Root cause</h2>
 * The insert maintenance path selects vector indexes with {@code READWRITE}
 * ({@code BucketInsertHandler.setVectorIndexes}, gates at lines 385 and 448), which excludes
 * {@code WAITING}. For a {@code WAITING} vector index an insert therefore writes no vector
 * ENTRIES key, no mutation-log record, and registers no post-commit graph add. The background
 * build only scans the frozen range {@code [lower, upper)} captured at boundary time, so writes
 * that land after that snapshot fall outside the build too. The on-heap graph is rebuilt from the
 * vector ENTRIES keyspace at startup and vector search reads that graph, so a document with no
 * ENTRIES key is permanently invisible to vector search. This is the same class of permanent loss
 * as the single-field and compound race.
 *
 * <h2>Fix</h2>
 * The vector write path selects vector indexes with {@code WRITABLE}, which includes {@code WAITING},
 * so a write that lands during the window writes its vector ENTRIES key and mutation-log record
 * synchronously. This test guards that behavior against regression.
 *
 * <h2>Determinism</h2>
 * Rather than racing a background boundary task, the test pins the index at {@code WAITING} by
 * creating it directly without scheduling the boundary task, then drops the metadata cache so the
 * insert handler observes the {@code WAITING} index. No build ever runs, so the synchronous write
 * path is the only thing that can write the ENTRIES keys, with no timing dependency.
 *
 * <h2>Expectation</h2>
 * Every stored document must have exactly one vector ENTRIES key. Before the fix the count was zero
 * while the documents were fully stored; with the {@code WRITABLE} write path it must equal N.
 */
class VectorIndexBuildingRaceHandlerTest extends BaseBucketHandlerTest {

    private void insertDocument(String label, float[] vector) {
        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString(label));
        BsonArray embedding = new BsonArray();
        for (float v : vector) {
            embedding.add(new BsonDouble(v));
        }
        doc.put("embedding", embedding);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.toBytes(doc)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        assertEquals(1, ((ArrayRedisMessage) msg).children().size(),
                "Each single-document insert must return exactly one ObjectId");
    }

    @Test
    void shouldDropVectorEntriesForInsertsArrivingWhileVectorIndexIsWaiting() {
        // Behavior: When a vector index is added to a populated bucket with BUCKET.INDEX, it is
        // created in WAITING status. Documents inserted while the index is WAITING must ALL end up
        // in the vector index. On the current engine the insert path selects vector indexes with
        // READWRITE, which excludes WAITING, so these documents are stored but never get a vector
        // ENTRIES key. Because the on-heap graph is rebuilt from the ENTRIES keyspace, the loss is
        // permanent: the documents are invisible to vector search forever.
        final int N = 15;

        // Single-shard bucket (vector indexes require single-shard) without any vector index yet.
        createBucket(TEST_BUCKET);

        // Pin a vector index at WAITING: create the index directory and definition directly, without
        // scheduling the boundary task, so the index never advances to BUILDING/READY and no build
        // backfills these writes. Then drop the cache so the insert handler reloads and observes it.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        VectorIndexDefinition definition = VectorIndexDefinition.create(
                "embedding_vector_idx", "embedding", 3, DistanceFunction.COSINE, IndexStatus.WAITING);

        DirectorySubspace vectorIndexSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            vectorIndexSubspace = VectorIndexUtil.create(tr, metadata.subspace(), definition);
            tr.commit().join();
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        // Insert N documents while the vector index is WAITING, each with a distinct embedding.
        for (int i = 0; i < N; i++) {
            insertDocument("doc-" + i, new float[]{0.1f * i, 0.2f * i, 0.3f * i});
        }

        // 1) Primary full scan must return ALL N documents (proves the documents are stored).
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf fullScanBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(fullScanBuf);
        List<BsonDocument> fullScanEntries = extractEntries(runCommand(channel, fullScanBuf));
        assertEquals(N, fullScanEntries.size(),
                "Primary full scan must return all inserted documents");

        // 2) Deterministic physical check: raw ENTRIES keys in the vector index subspace. The vector
        //    graph is rebuilt from this keyspace, so a missing key means permanent absence from
        //    vector search. On the current engine this is 0; it must be N.
        List<KeyValue> rawVectorEntries = TestUtil.fetchAllIndexedEntries(context, vectorIndexSubspace);
        assertEquals(N, rawVectorEntries.size(),
                "The vector index must contain one ENTRIES key per inserted document; documents "
                        + "inserted while the vector index is WAITING must not be dropped");
    }

    @Test
    void shouldMaintainVectorEntriesForDeletesWhileVectorIndexIsWaiting() {
        // Behavior: A document deleted while its bucket's vector index is WAITING must lose its vector
        // ENTRIES key. The delete maintenance path selects vector indexes with WRITABLE (which includes
        // WAITING), so the drop runs synchronously. Were it READWRITE, the WAITING index would be
        // skipped and the ENTRIES key would survive the delete, leaving a stale vector behind.
        final int N = 15;
        final int K = 5;

        createBucket(TEST_BUCKET);
        DirectorySubspace vectorIndexSubspace = pinWaitingVectorIndex();

        for (int i = 0; i < N; i++) {
            insertDocument("doc-" + i, new float[]{0.1f * i, 0.2f * i, 0.3f * i});
        }

        // Delete K documents while the vector index is WAITING, one label per call.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        for (int i = 0; i < K; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"label\": \"doc-" + i + "\"}").encode(buf);
            assertEquals(1, extractObjectIds(runCommand(channel, buf)).size(),
                    "Each delete must remove exactly one document");
        }

        List<KeyValue> rawVectorEntries = TestUtil.fetchAllIndexedEntries(context, vectorIndexSubspace);
        assertEquals(N - K, rawVectorEntries.size(),
                "Deletes during the WAITING window must drop one vector ENTRIES key per removed document");
    }

    @Test
    void shouldReindexVectorEntriesForUpdatesWhileVectorIndexIsWaiting() {
        // Behavior: Updating a document's vector field while its bucket's vector index is WAITING must
        // re-index it. The update maintenance path selects vector indexes with WRITABLE (which includes
        // WAITING), so the affected re-index runs synchronously and the ENTRIES value carries the new
        // vector. Were it READWRITE, the WAITING index would be skipped and the ENTRIES value would
        // retain the old vector, so an entry count alone cannot catch the regression.
        final int N = 15;
        final float[] newVector = new float[]{9.0f, 9.0f, 9.0f};

        createBucket(TEST_BUCKET);
        DirectorySubspace vectorIndexSubspace = pinWaitingVectorIndex();

        for (int i = 0; i < N; i++) {
            insertDocument("doc-" + i, new float[]{0.1f * i, 0.2f * i, 0.3f * i});
        }

        // Update every document's embedding to the same new vector while the index is WAITING.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        ByteBuf updateBuf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"embedding\": [9.0, 9.0, 9.0]}}").encode(updateBuf);
        assertEquals(N, extractObjectIds(runCommand(channel, updateBuf)).size(),
                "The update must touch every inserted document");

        // There must still be exactly one ENTRIES key per document, and each must carry the new vector.
        List<KeyValue> rawVectorEntries = TestUtil.fetchAllIndexedEntries(context, vectorIndexSubspace);
        assertEquals(N, rawVectorEntries.size(),
                "Updating the vector field must keep exactly one ENTRIES key per document");
        for (KeyValue kv : rawVectorEntries) {
            assertArrayEquals(newVector, VectorIndexValue.decode(kv.getValue()).vector(), 0.0f,
                    "The vector ENTRIES value must reflect the updated embedding, not the original");
        }
    }

    private DirectorySubspace pinWaitingVectorIndex() {
        // Create the index directory and definition directly in WAITING without scheduling the boundary
        // task, so it never advances and no build backfills the writes. Drop the cache so the write
        // handlers reload and observe the WAITING index.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        VectorIndexDefinition definition = VectorIndexDefinition.create(
                "embedding_vector_idx", "embedding", 3, DistanceFunction.COSINE, IndexStatus.WAITING);

        DirectorySubspace vectorIndexSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            vectorIndexSubspace = VectorIndexUtil.create(tr, metadata.subspace(), definition);
            tr.commit().join();
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);
        return vectorIndexSubspace;
    }
}
