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
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.CompoundIndexUtil;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ArrayRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduces the compound-index variant of the index-building race. Adding a compound index with
 * {@code BUCKET.INDEX} to an already populated, already cached bucket creates the index in
 * {@code WAITING} status and schedules a boundary task that later flips it to {@code BUILDING}
 * then {@code READY}. Documents written during that {@code WAITING} window can be permanently
 * absent from the compound index.
 *
 * <h2>Root cause</h2>
 * The write maintenance paths (insert, delete, update) selected indexes with {@code READWRITE},
 * which excludes {@code WAITING}. For a {@code WAITING} compound index a write therefore touched no
 * compound ENTRIES key. The background build only scans the frozen range {@code [lower, upper)}
 * captured at boundary time, so writes that land after that snapshot fall outside the build too,
 * and the document is permanently missing from the compound index. This is the same class of
 * permanent loss as the single-field and vector race.
 *
 * <h2>Fix</h2>
 * The write paths select indexes with {@code WRITABLE}, which includes {@code WAITING}, so a write
 * that lands during the window maintains its compound ENTRIES key synchronously. This test guards
 * that behavior against regression.
 *
 * <h2>Determinism</h2>
 * Rather than racing a background boundary task, the test pins the index at {@code WAITING} by
 * creating it directly without scheduling the boundary task, then drops the metadata cache so the
 * write handlers observe the {@code WAITING} index. No build ever runs, so the synchronous write
 * path is the only thing that can write the ENTRIES keys, with no timing dependency.
 *
 * <h2>Expectation</h2>
 * Every stored document must have exactly one compound ENTRIES key, and an update of an indexed
 * field must rewrite that key to carry the new value rather than leaving the old one behind.
 */
class CompoundIndexBuildingRaceHandlerTest extends BaseBucketHandlerTest {

    // The compound index covers (group, sk): group is the constant equality prefix, sk is the
    // trailing field that varies per document. The entry key tuple is
    // (ENTRIES, group, sk, objectId), so sk sits at tuple index 2 after unpacking. The index is
    // created without a collation, so string values are stored verbatim and decode cleanly.
    private static final String INDEX_NAME = "group_sk_compound_idx";
    private static final int SK_TUPLE_INDEX = 2;

    private void insertDocument(String group, String sk, String label) {
        BsonDocument doc = new BsonDocument();
        doc.put("group", new BsonString(group));
        doc.put("sk", new BsonString(sk));
        doc.put("label", new BsonString(label));

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.toBytes(doc)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        assertEquals(1, ((ArrayRedisMessage) msg).children().size(),
                "Each single-document insert must return exactly one ObjectId");
    }

    @Test
    void shouldDropCompoundEntriesForInsertsArrivingWhileCompoundIndexIsWaiting() {
        // Behavior: When a compound index is added to a populated bucket with BUCKET.INDEX, it is
        // created in WAITING status. Documents inserted while the index is WAITING must ALL end up
        // in the compound index. On the unfixed engine the insert path selects compound indexes with
        // READWRITE, which excludes WAITING, so these documents are stored but never get a compound
        // ENTRIES key, and the frozen build range never backfills them, so the loss is permanent.
        final int N = 15;

        createBucket(TEST_BUCKET);
        DirectorySubspace compoundIndexSubspace = pinWaitingCompoundIndex();

        for (int i = 0; i < N; i++) {
            insertDocument("grp", String.format("sk-%02d", i), "doc-" + i);
        }

        // 1) Primary full scan must return ALL N documents (proves the documents are stored).
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf fullScanBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(fullScanBuf);
        List<BsonDocument> fullScanEntries = extractEntries(runCommand(channel, fullScanBuf));
        assertEquals(N, fullScanEntries.size(),
                "Primary full scan must return all inserted documents");

        // 2) Deterministic physical check: raw ENTRIES keys in the compound index subspace. On the
        //    unfixed engine this is 0; it must be N.
        List<KeyValue> rawCompoundEntries = TestUtil.fetchAllIndexedEntries(context, compoundIndexSubspace);
        assertEquals(N, rawCompoundEntries.size(),
                "The compound index must contain one ENTRIES key per inserted document; documents "
                        + "inserted while the compound index is WAITING must not be dropped");
    }

    @Test
    void shouldMaintainCompoundEntriesForDeletesWhileCompoundIndexIsWaiting() {
        // Behavior: A document deleted while its bucket's compound index is WAITING must lose its
        // compound ENTRIES key. The delete maintenance path selects compound indexes with WRITABLE
        // (which includes WAITING), so the drop runs synchronously. Were it READWRITE, the WAITING
        // index would be skipped and the ENTRIES key would survive the delete, leaving a stale entry.
        final int N = 15;
        final int K = 5;

        createBucket(TEST_BUCKET);
        DirectorySubspace compoundIndexSubspace = pinWaitingCompoundIndex();

        for (int i = 0; i < N; i++) {
            insertDocument("grp", String.format("sk-%02d", i), "doc-" + i);
        }

        // Delete K documents while the compound index is WAITING, one label per call.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        for (int i = 0; i < K; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"label\": \"doc-" + i + "\"}").encode(buf);
            assertEquals(1, extractObjectIds(runCommand(channel, buf)).size(),
                    "Each delete must remove exactly one document");
        }

        List<KeyValue> rawCompoundEntries = TestUtil.fetchAllIndexedEntries(context, compoundIndexSubspace);
        assertEquals(N - K, rawCompoundEntries.size(),
                "Deletes during the WAITING window must drop one compound ENTRIES key per removed document");
    }

    @Test
    void shouldReindexCompoundEntriesForUpdatesWhileCompoundIndexIsWaiting() {
        // Behavior: Updating an indexed field while its bucket's compound index is WAITING must
        // re-index the document. The update maintenance path selects compound indexes with WRITABLE
        // (which includes WAITING), so the old ENTRIES key is removed and a new one is written with
        // the updated value. Were it READWRITE, the WAITING index would be skipped and the old key
        // would survive, so an entry count alone cannot catch the regression: the key must carry the
        // new value.
        final int N = 15;
        final String updatedSk = "sk-updated";

        createBucket(TEST_BUCKET);
        DirectorySubspace compoundIndexSubspace = pinWaitingCompoundIndex();

        for (int i = 0; i < N; i++) {
            insertDocument("grp", String.format("sk-%02d", i), "doc-" + i);
        }

        // Update every document's sk (a compound-index field) to the same new value while WAITING.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        ByteBuf updateBuf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"sk\": \"" + updatedSk + "\"}}").encode(updateBuf);
        assertEquals(N, extractObjectIds(runCommand(channel, updateBuf)).size(),
                "The update must touch every inserted document");

        // There must still be exactly one ENTRIES key per document, and each key must encode the new
        // sk value at tuple index 2, not the original one.
        List<KeyValue> rawCompoundEntries = TestUtil.fetchAllIndexedEntries(context, compoundIndexSubspace);
        assertEquals(N, rawCompoundEntries.size(),
                "Updating a compound-index field must keep exactly one ENTRIES key per document");
        for (KeyValue kv : rawCompoundEntries) {
            String sk = compoundIndexSubspace.unpack(kv.getKey()).getString(SK_TUPLE_INDEX);
            assertEquals(updatedSk, sk,
                    "The compound ENTRIES key must reflect the updated sk value, not the original");
        }
    }

    private DirectorySubspace pinWaitingCompoundIndex() {
        // Create the index directory and definition directly in WAITING without scheduling the
        // boundary task, so it never advances and no build backfills the writes. Drop the cache so
        // the write handlers reload and observe the WAITING index.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                INDEX_NAME,
                List.of(
                        new CompoundIndexField("group", BsonType.STRING, false),
                        new CompoundIndexField("sk", BsonType.STRING, false)
                ),
                IndexStatus.WAITING);

        DirectorySubspace compoundIndexSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            compoundIndexSubspace = CompoundIndexUtil.create(tr, metadata.subspace(), definition);
            tr.commit().join();
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);
        return compoundIndexSubspace;
    }
}
