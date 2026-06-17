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
import com.kronotop.Context;
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.bucket.index.VectorIndexUtil;
import com.kronotop.bucket.index.VectorIndexValue;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.BucketQueryArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Cross-node variant of the index-building race for VECTOR indexes. Mirrors
 * {@link SingleFieldIndexBuildingRaceMultiNodeTest} but the secondary index added with
 * {@code BUCKET.INDEX} is a vector index on {@code embedding}.
 *
 * <h2>Vector-specific scope</h2>
 * A document write stores its vector index ENTRY synchronously in the same FDB transaction as the
 * document (writes select indexes with {@code WRITABLE}, which includes {@code WAITING}). The on-heap
 * HNSW graph, by contrast, is built asynchronously after commit. This test verifies ONLY the FDB
 * ENTRIES, which are the transactional source of truth: every stored document must have exactly one
 * vector ENTRIES key. Graph integrity is intentionally out of scope here; it is non-ACID by design
 * and is covered by the vector crash-recovery tests.
 *
 * <p>Vector indexes require single-shard buckets, so the bucket is created on exactly one shard owned
 * by node2.
 *
 * <h2>Storage</h2>
 * A vector entry is keyed by {@code (ENTRIES, ObjectId)}; the value encodes the IndexEntry and the raw
 * vector via {@link VectorIndexValue}. The update test decodes each value to recover the stored vector.
 */
class VectorIndexBuildingRaceMultiNodeTest extends BaseBucketMultiNodeTest {
    private static final String TEST_BUCKET = "race-bucket-vector";
    private static final int NODE2_SHARD = 4;
    private static final int SEED_COUNT = 10;
    private static final int MAX_RACE_INSERTS = 3000;
    private static final String VECTOR_FIELD = "embedding";
    private static final String INDEX_SCHEMA =
            "{\"$vector\": {\"field\": \"" + VECTOR_FIELD + "\", \"dimensions\": 3, \"distance\": \"cosine\"}}";

    // Deterministic, exactly float-representable vectors so the JSON double round-trip is lossless.
    private static float[] seedVector(int seq) {
        return new float[]{seq, seq + 0.5f, seq + 0.25f};
    }

    private static float[] updatedVector(int seq) {
        return new float[]{seq + 100000f, seq + 100000.5f, seq + 100000.25f};
    }

    private boolean insertDocument(io.netty.channel.embedded.EmbeddedChannel channel, int seq) {
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        insertCmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(documentJson(seq, seedVector(seq)))).encode(buf);
        Object response = runCommand(channel, buf);
        // A successful single-document insert returns exactly one ObjectId; conflicts or rejects do not.
        return response instanceof ArrayRedisMessage array && array.children().size() == 1;
    }

    private static String documentJson(int id, float[] vector) {
        return String.format("{\"id\": %d, \"%s\": [%s, %s, %s]}",
                id, VECTOR_FIELD,
                Float.toString(vector[0]), Float.toString(vector[1]), Float.toString(vector[2]));
    }

    private DirectorySubspace indexSubspaceIfReady(Context ctx, String namespace) {
        try (Transaction tr = ctx.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(ctx, tr, namespace, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexBySelector(VECTOR_FIELD, IndexSelectionPolicy.ALL);
            if (index == null) {
                return null;
            }
            IndexStatus status = VectorIndexUtil.loadIndexDefinition(tr, index.subspace()).status();
            return status == IndexStatus.READY ? index.subspace() : null;
        }
    }

    private void createBucketOnNode2() {
        BucketCommandBuilder<String, String> node2Cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        node2Cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.shards(List.of(NODE2_SHARD))).encode(buf);
        Object response = runCommand(node2.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private void createIndexOnNode1() {
        BucketCommandBuilder<String, String> node1Cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        node1Cmd.indexCreate(TEST_BUCKET, INDEX_SCHEMA).encode(buf);
        Object response = runCommand(node1.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
    }

    private DirectorySubspace awaitIndexReady(String namespace) {
        // Poll via node1's context so node2's cache is not refreshed early (which would mask the race).
        AtomicReference<DirectorySubspace> readySubspace = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            DirectorySubspace subspace = indexSubspaceIfReady(node1.getContext(), namespace);
            if (subspace != null) {
                readySubspace.set(subspace);
                return true;
            }
            return false;
        });
        return readySubspace.get();
    }

    private List<BsonDocument> fullScan() {
        BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf helloBuf = Unpooled.buffer();
        queryCmd.hello(3).encode(helloBuf);
        runCommand(node2.getChannel(), helloBuf);

        ByteBuf queryBuf = Unpooled.buffer();
        queryCmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(SEED_COUNT + MAX_RACE_INSERTS)).encode(queryBuf);
        return extractEntries(runCommand(node2.getChannel(), queryBuf));
    }

    private static String vectorKey(float[] vector) {
        StringBuilder sb = new StringBuilder();
        for (float v : vector) {
            sb.append(v).append(',');
        }
        return sb.toString();
    }

    @Test
    void shouldIndexAllDocumentsInsertedRemotelyDuringIndexBuildingWindow() throws InterruptedException {
        // Behavior: When node1 adds a VECTOR index with BUCKET.INDEX to a bucket that node2 already has
        // cached, documents that node2 inserts while the index is still building must ALL get a vector
        // ENTRIES key. Remote inserts racing the WAITING/BUILDING transition must not be permanently
        // dropped. The raw ENTRIES count must account for every stored document.
        String namespace = node2.getContext().getConfig().getString("default_namespace");

        createBucketOnNode2();

        // Seed documents through node2 so node2 caches the bucket metadata WITHOUT the index.
        for (int i = 0; i < SEED_COUNT; i++) {
            assertTrue(insertDocument(node2.getChannel(), i), "Seed insert must succeed");
        }

        // Background: node2 keeps inserting (distinct vectors) until the index is READY.
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Integer> raceInserted = new AtomicReference<>(0);
        Thread inserter = new Thread(() -> {
            int seq = SEED_COUNT;
            int ok = 0;
            try {
                while (!stop.get() && (seq - SEED_COUNT) < MAX_RACE_INSERTS) {
                    if (insertDocument(node2.getChannel(), seq)) {
                        ok++;
                    }
                    seq++;
                }
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                raceInserted.set(ok);
            }
        }, "node2-racing-inserter");
        inserter.start();

        createIndexOnNode1();
        DirectorySubspace readySubspace = awaitIndexReady(namespace);

        stop.set(true);
        inserter.join();
        assertNull(failure.get(), "Racing inserter must not throw: " + failure.get());
        assertTrue(raceInserted.get() > 0, "At least one racing insert must have succeeded");

        List<BsonDocument> storedDocuments = fullScan();
        int storedCount = storedDocuments.size();
        assertEquals(SEED_COUNT + raceInserted.get(), storedCount,
                "Full scan must return every successfully inserted document");

        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(node1.getContext(), readySubspace);
        assertEquals(storedCount, indexEntries.size(),
                "The vector index must contain one ENTRIES key per stored document; documents inserted "
                        + "remotely during the BUILDING window must not be dropped");
    }

    @Test
    void shouldNotResurrectRemotelyDeletedDocumentsDuringIndexBuildingWindow() throws InterruptedException {
        // Behavior: When node1 adds a VECTOR index to a bucket that node2 has cached, documents that
        // node2 DELETES while the index is still building must NOT be resurrected by the background
        // build. The build scans the frozen range [lower, upper) captured at boundary time; a document
        // deleted during that window must not reappear as a stale vector ENTRIES key. The raw ENTRIES
        // count must equal the number of documents that actually remain.
        final int SEED = 200;
        String namespace = node2.getContext().getConfig().getString("default_namespace");

        createBucketOnNode2();

        for (int i = 0; i < SEED; i++) {
            assertTrue(insertDocument(node2.getChannel(), i), "Seed insert must succeed");
        }

        // Background: node2 keeps deleting seeded documents (idempotent re-attempts) until READY.
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread deleter = new Thread(() -> {
            int seq = 0;
            try {
                while (!stop.get()) {
                    deleteDocument(node2.getChannel(), seq % SEED);
                    seq++;
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "node2-racing-deleter");
        deleter.start();

        createIndexOnNode1();
        DirectorySubspace readySubspace = awaitIndexReady(namespace);

        stop.set(true);
        deleter.join();
        assertNull(failure.get(), "Racing deleter must not throw: " + failure.get());

        List<BsonDocument> storedDocuments = fullScan();
        int storedCount = storedDocuments.size();
        assertTrue(storedCount < SEED, "At least one racing delete must have landed during the window");

        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(node1.getContext(), readySubspace);
        assertEquals(storedCount, indexEntries.size(),
                "The vector index must contain exactly one ENTRIES key per remaining document; a document "
                        + "deleted during the BUILDING window must not be resurrected by the build");
    }

    @Test
    void shouldNotResurrectStaleEntriesForRemotelyUpdatedDocumentsDuringIndexBuildingWindow() throws InterruptedException {
        // Behavior: When node1 adds a VECTOR index to a bucket that node2 has cached, the vectors of
        // documents that node2 UPDATES while the index is still building must be reflected in the index.
        // The build must not overwrite a freshly-updated entry with the stale vector from its frozen
        // snapshot. The multiset of vectors decoded from the index ENTRIES values must exactly match the
        // multiset of current document vectors: none missing, none superseded value resurrected.
        final int SEED = 100;
        String namespace = node2.getContext().getConfig().getString("default_namespace");

        createBucketOnNode2();

        for (int i = 0; i < SEED; i++) {
            assertTrue(insertDocument(node2.getChannel(), i), "Seed insert must succeed");
        }

        // Background: node2 rewrites each seed's embedding to a distinct, far-away vector until READY.
        // After the first pass the filtered re-attempts match nothing and are no-ops, keeping the
        // window busy.
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread updater = new Thread(() -> {
            int seq = 0;
            try {
                while (!stop.get()) {
                    updateDocument(node2.getChannel(), seq % SEED);
                    seq++;
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "node2-racing-updater");
        updater.start();

        createIndexOnNode1();
        DirectorySubspace readySubspace = awaitIndexReady(namespace);

        stop.set(true);
        updater.join();
        assertNull(failure.get(), "Racing updater must not throw: " + failure.get());

        List<BsonDocument> storedDocuments = fullScan();
        assertEquals(SEED, storedDocuments.size(), "An update must not change the document count");

        List<String> currentVectors = new ArrayList<>();
        long updatedCount = 0;
        for (BsonDocument doc : storedDocuments) {
            float[] vector = readEmbedding(doc);
            currentVectors.add(vectorKey(vector));
            if (vector[0] >= 100000f) {
                updatedCount++;
            }
        }
        assertTrue(updatedCount > 0, "At least one racing update must have landed during the window");
        Collections.sort(currentVectors);

        // Decode each vector ENTRIES value and compare the multiset of vectors to the current documents'.
        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(node1.getContext(), readySubspace);
        List<String> indexedVectors = new ArrayList<>();
        for (KeyValue kv : indexEntries) {
            indexedVectors.add(vectorKey(VectorIndexValue.decode(kv.getValue()).vector()));
        }
        Collections.sort(indexedVectors);

        assertEquals(currentVectors, indexedVectors,
                "The vector index ENTRIES vectors must exactly match the current document vectors; the "
                        + "build must not resurrect a superseded vector for a document updated during the window");
    }

    private static float[] readEmbedding(BsonDocument doc) {
        org.bson.BsonArray arr = doc.getArray(VECTOR_FIELD);
        float[] vector = new float[arr.size()];
        for (int i = 0; i < arr.size(); i++) {
            vector[i] = (float) arr.get(i).asNumber().doubleValue();
        }
        return vector;
    }

    private void deleteDocument(io.netty.channel.embedded.EmbeddedChannel channel, int seq) {
        BucketCommandBuilder<byte[], byte[]> deleteCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        deleteCmd.delete(TEST_BUCKET, String.format("{\"id\": %d}", seq)).encode(buf);
        runCommand(channel, buf);
    }

    private void updateDocument(io.netty.channel.embedded.EmbeddedChannel channel, int seq) {
        float[] v = updatedVector(seq);
        BucketCommandBuilder<byte[], byte[]> updateCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        updateCmd.update(TEST_BUCKET,
                String.format("{\"id\": %d}", seq),
                String.format("{\"$set\": {\"%s\": [%s, %s, %s]}}",
                        VECTOR_FIELD, Float.toString(v[0]), Float.toString(v[1]), Float.toString(v[2]))).encode(buf);
        runCommand(channel, buf);
    }
}
