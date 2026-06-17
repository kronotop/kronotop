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
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.index.SingleFieldIndexUtil;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ArrayRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduces a correctness bug in inline index creation via {@code BUCKET.CREATE}: documents
 * inserted right after a bucket is created with an inline SINGLE_FIELD index can be permanently
 * missing from that index. This violates the invariant that an index never changes query semantics
 * and is only an accelerator: an index-backed scan returns fewer rows than exist.
 *
 * <h2>Root cause</h2>
 * An inline index is created in {@code READY} status, so there is no background build for it. While
 * handling {@code BUCKET.CREATE} the bucket metadata snapshot is cached before the inline index is
 * registered in that snapshot, so the cached metadata does not contain the new index. Until the
 * asynchronous metadata-updated event invalidates the cache, inserts read the stale cached metadata,
 * never see the index, and write no index entry. Because the index is already {@code READY}, nothing
 * backfills them, so the loss is permanent. The earliest inserts, which race the cache refresh, are
 * the ones dropped.
 *
 * <h2>Fix</h2>
 * {@code BucketCreateHandler} reloads bucket metadata from FoundationDB after creating inline
 * indexes, so the cache exposes them before any insert runs.
 *
 * <h2>Expectation</h2>
 * Every stored document must have exactly one index entry. The test asserts this against the raw
 * ENTRIES keyspace and an index-backed scan; both fail on the unfixed engine while the full scan
 * still returns every document.
 */
class SingleFieldIndexBuildingRaceHandlerTest extends BaseBucketHandlerTest {

    @Test
    void shouldIndexAllDocumentsInsertedBeforeMetadataCacheRefresh() {
        // Behavior: Documents inserted right after a bucket is created with an inline SINGLE_FIELD
        // string index must ALL be present in that index. Inserts that race the post-create
        // metadata cache refresh must not be permanently dropped. Both the raw index ENTRIES and an
        // index-backed scan must account for every inserted document.
        final int N = 20;

        // Create the bucket WITH an inline single-field string index on "sk", then insert
        // immediately, before the post-create metadata cache refresh has necessarily landed.
        createBucket(TEST_BUCKET, List.of(TEST_SHARD_ID), "{\"sk\": {\"bson_type\": \"string\"}}");

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // Immediately insert N docs, one per BUCKET.INSERT (separate auto-commit transactions),
        // with distinct lexicographically-sortable "sk" values, racing the metadata cache refresh.
        for (int i = 0; i < N; i++) {
            ByteBuf insertBuf = Unpooled.buffer();
            byte[] doc = BSONUtil.jsonToDocumentThenBytes(String.format("{\"sk\": \"sk-%02d\"}", i));
            insertCmd.insert(TEST_BUCKET, doc).encode(insertBuf);
            Object insertMsg = runCommand(channel, insertBuf);
            assertInstanceOf(ArrayRedisMessage.class, insertMsg);
            assertEquals(1, ((ArrayRedisMessage) insertMsg).children().size(),
                    "Each single-document insert must return exactly one ObjectId");
        }

        // Read the final index state: the inline index is already READY, so this confirms the
        // index subspace and drops any stale cache entry before the verification queries.
        Index skIndex;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            skIndex = metadata.indexes().getIndex("sk", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(skIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        switchProtocol(cmd, RESPVersion.RESP3);

        // 1) Primary full scan must return ALL N documents (proves no document was lost).
        ByteBuf fullScanBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(fullScanBuf);
        List<BsonDocument> fullScanEntries = extractEntries(runCommand(channel, fullScanBuf));
        assertEquals(N, fullScanEntries.size(),
                "Primary full scan must return all inserted documents");

        // 2) Deterministic physical check: raw ENTRIES keys in the "sk" index subspace.
        //    Immune to query-planner/cursor behavior. On the buggy code this is < N.
        List<KeyValue> rawIndexEntries = TestUtil.fetchAllIndexedEntries(context, skIndex.subspace());
        assertEquals(N, rawIndexEntries.size(),
                "The secondary index must contain one ENTRIES key per inserted document; "
                        + "documents inserted before the post-create cache refresh must not be dropped");

        // 3) End-to-end reproduction: index-backed sorted scan must also return all N.
        ByteBuf indexScanBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"sk\": {\"$gte\": \"\"}}",
                new BucketQueryArgs().sortBy("sk", "ASC")).encode(indexScanBuf);
        List<BsonDocument> indexScanEntries = extractEntries(runCommand(channel, indexScanBuf));
        assertEquals(N, indexScanEntries.size(),
                "Index-backed scan must return all inserted documents");
    }

    @Test
    void shouldMaintainSingleFieldEntriesForDeletesWhileIndexIsWaiting() {
        // Behavior: A document deleted while its bucket's single-field index is WAITING must lose its
        // index ENTRIES key. The delete maintenance path selects indexes with WRITABLE (which includes
        // WAITING), so the drop runs synchronously. Were it READWRITE, the WAITING index would be
        // skipped and the ENTRIES key would survive the delete, leaving a stale entry behind.
        final int N = 15;
        final int K = 5;

        createBucket(TEST_BUCKET);
        DirectorySubspace skIndexSubspace = pinWaitingSingleFieldIndex();

        for (int i = 0; i < N; i++) {
            insertDocument(String.format("sk-%02d", i));
        }

        // Delete K documents while the index is WAITING, one sk value per call.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        for (int i = 0; i < K; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, String.format("{\"sk\": \"sk-%02d\"}", i)).encode(buf);
            assertEquals(1, extractObjectIds(runCommand(channel, buf)).size(),
                    "Each delete must remove exactly one document");
        }

        List<KeyValue> rawIndexEntries = TestUtil.fetchAllIndexedEntries(context, skIndexSubspace);
        assertEquals(N - K, rawIndexEntries.size(),
                "Deletes during the WAITING window must drop one index ENTRIES key per removed document");
    }

    @Test
    void shouldReindexSingleFieldEntriesForUpdatesWhileIndexIsWaiting() {
        // Behavior: Updating the indexed field while its bucket's single-field index is WAITING must
        // re-index the document. The update maintenance path selects indexes with WRITABLE (which
        // includes WAITING), so the old ENTRIES key is removed and a new one is written with the
        // updated value. Were it READWRITE, the WAITING index would be skipped and the old key would
        // survive, so an entry count alone cannot catch the regression: the key must carry the new
        // value.
        final int N = 15;
        final String updatedSk = "sk-updated";

        createBucket(TEST_BUCKET);
        DirectorySubspace skIndexSubspace = pinWaitingSingleFieldIndex();

        for (int i = 0; i < N; i++) {
            insertDocument(String.format("sk-%02d", i));
        }

        // Update every document's sk (the indexed field) to the same new value while WAITING.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        ByteBuf updateBuf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"sk\": \"" + updatedSk + "\"}}").encode(updateBuf);
        assertEquals(N, extractObjectIds(runCommand(channel, updateBuf)).size(),
                "The update must touch every inserted document");

        // There must still be exactly one ENTRIES key per document, and each key must encode the new
        // sk value at tuple index 1, not the original one. The entry key is (ENTRIES, indexValue,
        // ObjectId), and the index has no collation, so the value decodes verbatim.
        List<KeyValue> rawIndexEntries = TestUtil.fetchAllIndexedEntries(context, skIndexSubspace);
        assertEquals(N, rawIndexEntries.size(),
                "Updating the indexed field must keep exactly one ENTRIES key per document");
        for (KeyValue kv : rawIndexEntries) {
            String sk = skIndexSubspace.unpack(kv.getKey()).getString(1);
            assertEquals(updatedSk, sk,
                    "The index ENTRIES key must reflect the updated sk value, not the original");
        }
    }

    private DirectorySubspace pinWaitingSingleFieldIndex() {
        // Create the index directory and definition directly in WAITING without scheduling the
        // boundary task, so it never advances and no build backfills the writes. Drop the cache so
        // the write handlers reload and observe the WAITING index.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "sk_idx", "sk", BsonType.STRING, false, IndexStatus.WAITING);

        DirectorySubspace skIndexSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            skIndexSubspace = SingleFieldIndexUtil.create(tr, metadata.subspace(), definition);
            tr.commit().join();
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);
        return skIndexSubspace;
    }

    private void insertDocument(String sk) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] doc = BSONUtil.jsonToDocumentThenBytes(String.format("{\"sk\": \"%s\"}", sk));
        cmd.insert(TEST_BUCKET, doc).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        assertEquals(1, ((ArrayRedisMessage) msg).children().size(),
                "Each single-document insert must return exactly one ObjectId");
    }
}
