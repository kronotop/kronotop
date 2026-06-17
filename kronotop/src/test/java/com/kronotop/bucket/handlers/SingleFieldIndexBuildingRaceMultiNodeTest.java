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
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexUtil;
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
 * Reproduces the cross-node variant of the index-building race. Unlike
 * {@code IndexBuildingRaceHandlerTest} (inline index via {@code BUCKET.CREATE} on a single node),
 * here the bucket already exists and is cached on a remote node before a SINGLE_FIELD index is
 * added with {@code BUCKET.INDEX} from a different node.
 *
 * <h2>Setup (facts)</h2>
 * <ul>
 *   <li>The bucket lives on a shard owned by node2. node2 caches the bucket metadata (without the
 *       secondary index) by inserting seed documents first.</li>
 *   <li>node1 issues {@code BUCKET.INDEX} to create a single-field string index on {@code sk}. The
 *       index is created in {@code WAITING} status; a boundary task then captures the build range
 *       {@code [lower, upper)} and flips the index to {@code BUILDING}, then {@code READY}.</li>
 *   <li>Concurrently, node2 keeps inserting documents with distinct {@code sk} values until the
 *       index becomes {@code READY}.</li>
 * </ul>
 *
 * <h2>Root cause</h2>
 * node2 only learns about the new index through the asynchronous metadata-updated event, and even
 * once it observes the index it sees {@code WAITING}. Document writes previously selected indexes
 * with {@code READWRITE}, which excludes {@code WAITING}, so node2 wrote no index entry for inserts
 * it committed after the boundary snapshot but before it observed {@code BUILDING}. The build range
 * {@code [lower, upper)} was frozen before those documents existed, so the build does not cover them
 * either. They are permanently missing from the index.
 *
 * <h2>Fix</h2>
 * Document write maintenance selects indexes with {@code WRITABLE}, which includes {@code WAITING}.
 * The boundary routine already converges cluster metadata before capturing the build range, so once
 * {@code WAITING} indexes are written to, that convergence point becomes a clean fence: writes
 * before it fall inside the build range, writes after it are indexed synchronously.
 *
 * <h2>Expectation</h2>
 * Every stored document must have exactly one {@code sk} index entry. On the unfixed engine the raw
 * ENTRIES count is fewer than the number of stored documents; the assertion fails and gates the bug.
 */
class SingleFieldIndexBuildingRaceMultiNodeTest extends BaseBucketMultiNodeTest {
    private static final String TEST_BUCKET = "race-bucket";
    private static final int NODE2_SHARD = 4;
    private static final int SEED_COUNT = 10;
    private static final int MAX_RACE_INSERTS = 3000;
    private static final String INDEX_SCHEMA = "{\"sk\": {\"bson_type\": \"string\"}}";

    private boolean insertDocument(io.netty.channel.embedded.EmbeddedChannel channel, int seq) {
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] doc = BSONUtil.jsonToDocumentThenBytes(String.format("{\"sk\": \"sk-%08d\"}", seq));
        insertCmd.insert(TEST_BUCKET, doc).encode(buf);
        Object response = runCommand(channel, buf);
        // A successful single-document insert returns exactly one ObjectId; conflicts or rejects do not.
        return response instanceof ArrayRedisMessage array && array.children().size() == 1;
    }

    private DirectorySubspace indexSubspaceIfReady(Context ctx, String namespace) {
        try (Transaction tr = ctx.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(ctx, tr, namespace, TEST_BUCKET);
            Index index = metadata.indexes().getIndex("sk", IndexSelectionPolicy.ALL);
            if (index == null) {
                return null;
            }
            IndexStatus status = SingleFieldIndexUtil.loadIndexDefinition(tr, index.subspace()).status();
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

    @Test
    void shouldIndexAllDocumentsInsertedRemotelyDuringIndexBuildingWindow() throws InterruptedException {
        // Behavior: When node1 adds a SINGLE_FIELD index with BUCKET.INDEX to a bucket that node2
        // already has cached, documents that node2 inserts while the index is still building must
        // ALL end up in the secondary index. Remote inserts racing the WAITING/BUILDING transition
        // must not be permanently dropped. The raw index ENTRIES count must account for every
        // stored document.
        String namespace = node2.getContext().getConfig().getString("default_namespace");

        createBucketOnNode2();

        // Seed documents through node2 so node2 caches the bucket metadata WITHOUT the index.
        for (int i = 0; i < SEED_COUNT; i++) {
            assertTrue(insertDocument(node2.getChannel(), i), "Seed insert must succeed");
        }

        // Background: node2 keeps inserting (distinct sk values) until the index is READY.
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

        // Ground truth: number of stored documents (shard 4 is local to node2).
        List<BsonDocument> storedDocuments = fullScan();
        int storedCount = storedDocuments.size();
        assertEquals(SEED_COUNT + raceInserted.get(), storedCount,
                "Full scan must return every successfully inserted document");

        // Deterministic physical check: one ENTRIES key per stored document in the "sk" index.
        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(node1.getContext(), readySubspace);
        assertEquals(storedCount, indexEntries.size(),
                "The secondary index must contain one ENTRIES key per stored document; documents "
                        + "inserted remotely during the BUILDING window must not be dropped");
    }

    @Test
    void shouldNotResurrectRemotelyDeletedDocumentsDuringIndexBuildingWindow() throws InterruptedException {
        // Behavior: When node1 adds a SINGLE_FIELD index with BUCKET.INDEX to a bucket that node2 has
        // cached, documents that node2 DELETES while the index is still building must NOT be resurrected
        // by the background build. The build scans the frozen range [lower, upper) captured at boundary
        // time; a document deleted during that window must not reappear as a stale index entry. The raw
        // ENTRIES count must equal the number of documents that actually remain.
        final int SEED = 200;
        String namespace = node2.getContext().getConfig().getString("default_namespace");

        createBucketOnNode2();

        // Seed documents through node2 so node2 caches the bucket metadata WITHOUT the index.
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

        // Ground truth: documents that remain after the race (shard 4 is local to node2).
        List<BsonDocument> storedDocuments = fullScan();
        int storedCount = storedDocuments.size();
        assertTrue(storedCount < SEED, "At least one racing delete must have landed during the window");

        // Deterministic physical check: one ENTRIES key per REMAINING document, none resurrected.
        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(node1.getContext(), readySubspace);
        assertEquals(storedCount, indexEntries.size(),
                "The secondary index must contain exactly one ENTRIES key per remaining document; a "
                        + "document deleted during the BUILDING window must not be resurrected by the build");
    }

    @Test
    void shouldNotResurrectStaleEntriesForRemotelyUpdatedDocumentsDuringIndexBuildingWindow() throws InterruptedException {
        // Behavior: When node1 adds a SINGLE_FIELD index with BUCKET.INDEX to a bucket that node2 has
        // cached, the indexed field of documents that node2 UPDATES while the index is still building
        // must be reflected in the index. The build must not overwrite a freshly-updated entry with the
        // stale value from its frozen snapshot. The set of indexed values must equal the set of current
        // document values: no value is missing and no superseded value is resurrected.
        final int SEED = 100;
        String namespace = node2.getContext().getConfig().getString("default_namespace");

        createBucketOnNode2();

        // Seed documents with sk = "sk-%08d" so node2 caches the bucket metadata WITHOUT the index.
        for (int i = 0; i < SEED; i++) {
            assertTrue(insertDocument(node2.getChannel(), i), "Seed insert must succeed");
        }

        // Background: node2 rewrites each seed's sk from "sk-%08d" to "up-%08d" until READY. After the
        // first pass the filtered re-attempts match nothing and are no-ops, keeping the window busy.
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

        // Ground truth: current document sk values (shard 4 is local to node2).
        List<BsonDocument> storedDocuments = fullScan();
        assertEquals(SEED, storedDocuments.size(), "An update must not change the document count");

        List<String> currentSk = new ArrayList<>();
        long updatedCount = 0;
        for (BsonDocument doc : storedDocuments) {
            String sk = doc.getString("sk").getValue();
            currentSk.add(sk);
            if (sk.startsWith("up-")) {
                updatedCount++;
            }
        }
        assertTrue(updatedCount > 0, "At least one racing update must have landed during the window");
        Collections.sort(currentSk);

        // The index ENTRIES values, decoded from the (ENTRIES, indexValue, ObjectId) key at tuple
        // index 1, must exactly match the current document sk values.
        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(node1.getContext(), readySubspace);
        List<String> indexedSk = new ArrayList<>();
        for (KeyValue kv : indexEntries) {
            indexedSk.add(readySubspace.unpack(kv.getKey()).getString(1));
        }
        Collections.sort(indexedSk);

        assertEquals(currentSk, indexedSk,
                "The index ENTRIES values must exactly match the current document sk values; the build "
                        + "must not resurrect a superseded value for a document updated during the window");
    }

    private void deleteDocument(io.netty.channel.embedded.EmbeddedChannel channel, int seq) {
        BucketCommandBuilder<byte[], byte[]> deleteCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        deleteCmd.delete(TEST_BUCKET, String.format("{\"sk\": \"sk-%08d\"}", seq)).encode(buf);
        runCommand(channel, buf);
    }

    private void updateDocument(io.netty.channel.embedded.EmbeddedChannel channel, int seq) {
        BucketCommandBuilder<byte[], byte[]> updateCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        updateCmd.update(TEST_BUCKET,
                String.format("{\"sk\": \"sk-%08d\"}", seq),
                String.format("{\"$set\": {\"sk\": \"up-%08d\"}}", seq)).encode(buf);
        runCommand(channel, buf);
    }
}
