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

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TestUtil;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class SingleFieldIndexBuildingRoutineTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldBuildIndexAtBackground() {
        // Behavior: Creating an index after documents exist triggers background index building
        // that populates back pointers for all existing documents.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 32}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 40}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<ObjectId> expectedObjectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(2, expectedObjectIds.size());

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "test-index",
                "age",
                BsonType.INT32
                , false, IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<Long> expectedIndexValues = new ArrayList<>(List.of(32L, 40L));
            List<Long> indexValues = new ArrayList<>();
            List<ObjectId> objectIds = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    // Back pointer key: (BACK_POINTER, ObjectId bytes, indexValue)
                    byte[] objectIdBytes = unpacked.getBytes(1);
                    objectIds.add(new ObjectId(objectIdBytes));
                    indexValues.add(unpacked.getLong(2));
                }
                return expectedObjectIds.equals(objectIds)
                        && expectedIndexValues.equals(indexValues);
            }
        });

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            AtomicInteger counter = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TaskStorage.tasks(tr, taskSubspace, (id) -> {
                    counter.incrementAndGet();
                    return true;
                });
                return counter.get() == 0; // all swept and cleaned
            }
        });
    }

    @Test
    void shouldNotDoubleCountCardinalityWhenEntriesAlreadyIndexedByOnlineWrites() {
        // Behavior: When the index is created first, the online write path (WRITABLE) indexes the
        // inserted documents while the index is still building. Those same documents also fall within
        // the background builder's scan range. The builder must skip ObjectIds already indexed so that
        // cardinality is not double-counted: exactly one entry per document and cardinality equals the
        // document count.

        // Create the index first so the online insert path maintains it during the build.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "age-dedup-index",
                "age",
                BsonType.INT32
                , false, IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Insert documents through the online path; these are indexed online and also fall within the
        // background builder's scan range, creating the overlap the dedup logic must handle.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 10}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 20}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 30}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        assertEquals(3, TestUtil.extractObjectIds((ArrayRedisMessage) msg).size());

        // Wait for the background build task to complete and be swept.
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            AtomicInteger counter = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TaskStorage.tasks(tr, taskSubspace, (id) -> {
                    counter.incrementAndGet();
                    return true;
                });
                return counter.get() == 0;
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);

            // Exactly one index entry per document — the builder did not re-create online-written entries.
            byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            byte[] end = ByteArrayUtil.strinc(begin);
            List<KeyValue> entries = tr.getRange(begin, end).asList().join();
            assertEquals(3, entries.size(), "Should have exactly one index entry per document");

            // Cardinality must be exact, not double-counted by the builder.
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(definition.id());
            assertNotNull(stats, "Index statistics should be present");
            assertEquals(3L, stats.cardinality(), "Cardinality must equal document count, not double counted");
        }
    }

    @Test
    void shouldFailIndexBuildingWhenTypeMismatchOccurs() {
        // Insert documents with STRING values for the 'age' field
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": \"thirty-two\"}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": \"forty\"}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        // Create an index expecting INT32 for the 'age' field
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "age-index",
                "age",
                BsonType.INT32
                , false, IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        Versionstamp taskId = TestUtil.findIndexMaintenanceTaskId(context, taskSubspace, IndexMaintenanceTaskKind.BUILD);

        // Wait for the task to fail due to the type mismatch
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
                return state.status() == IndexTaskStatus.FAILED;
            }
        });

        // Verify an error message contains type mismatch information
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(IndexTaskStatus.FAILED, state.status());
            assertNotNull(state.error(), "Error message should be set");
            assertTrue(state.error().contains("Index type mismatch"),
                    "Error message should indicate type mismatch");
            assertTrue(state.error().contains("age-index"),
                    "Error message should mention the index name");
            assertTrue(state.error().contains("INT32"),
                    "Error message should mention expected type");
            assertTrue(state.error().contains("STRING"),
                    "Error message should mention actual type");
        }
    }

    @Test
    void shouldBuildMultikeyIndexAtBackground() {
        // Insert documents with arrays of objects
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"scores\": [{\"type\": \"math\"}, {\"type\": \"english\"}]}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"scores\": [{\"type\": \"science\"}, {\"type\": \"history\"}]}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        // Create a multikey index on "scores.type"
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "scores-type-index",
                "scores.type",
                BsonType.STRING
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for the index to be built and verify entries
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<String> expectedIndexValues = List.of("english", "history", "math", "science");
            List<String> indexValues = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    indexValues.add(unpacked.getString(1));
                }
                indexValues.sort(String::compareTo);
                return expectedIndexValues.equals(indexValues);
            }
        });
    }

    @Test
    void shouldDeduplicateMultikeyIndexEntriesDuringBackgroundBuild() {
        // Insert documents with duplicate values in arrays
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        // Document has 4 array elements but only 2 unique values
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"tags\": [{\"name\": \"java\"}, {\"name\": \"java\"}, {\"name\": \"kotlin\"}, {\"name\": \"java\"}]}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Create a multikey index on "tags.name"
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "tags-name-index",
                "tags.name",
                BsonType.STRING
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for the index to be built and verify only unique entries exist
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<String> indexValues = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    indexValues.add(unpacked.getString(1));
                }
                // Should have only 2 entries due to deduplication
                return indexValues.size() == 2
                        && indexValues.contains("java")
                        && indexValues.contains("kotlin");
            }
        });
    }

    @Test
    void shouldTrackCardinalityCorrectlyForMultikeyIndexDuringBackgroundBuild() {
        // Insert multiple documents with arrays
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        // Doc1: 2 unique values
                        BSONUtil.jsonToDocumentThenBytes("{\"items\": [{\"color\": \"red\"}, {\"color\": \"blue\"}]}"),
                        // Doc2: 3 unique values
                        BSONUtil.jsonToDocumentThenBytes("{\"items\": [{\"color\": \"green\"}, {\"color\": \"yellow\"}, {\"color\": \"purple\"}]}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Create a multikey index on "items.color"
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "items-color-index",
                "items.color",
                BsonType.STRING
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for the index to be built and verify cardinality
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
                IndexStatistics stats = indexStatistics.get(definition.id());
                // Cardinality should be 5 (2 + 3)
                return stats != null && stats.cardinality() == 5L;
            }
        });
    }

    @Test
    void shouldDeduplicateAndTrackCardinalityCorrectlyForMultikeyIndexDuringBackgroundBuild() {
        // Insert multiple documents with duplicates in arrays
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        // Doc1: 4 elements, 2 unique values (java x3, kotlin x1)
                        BSONUtil.jsonToDocumentThenBytes("{\"langs\": [{\"name\": \"java\"}, {\"name\": \"java\"}, {\"name\": \"kotlin\"}, {\"name\": \"java\"}]}"),
                        // Doc2: 5 elements, 3 unique values (python x2, rust x2, go x1)
                        BSONUtil.jsonToDocumentThenBytes("{\"langs\": [{\"name\": \"python\"}, {\"name\": \"rust\"}, {\"name\": \"python\"}, {\"name\": \"go\"}, {\"name\": \"rust\"}]}"),
                        // Doc3: 3 elements, 1 unique value (scala x3)
                        BSONUtil.jsonToDocumentThenBytes("{\"langs\": [{\"name\": \"scala\"}, {\"name\": \"scala\"}, {\"name\": \"scala\"}]}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Create a multikey index on "langs.name"
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "langs-name-index",
                "langs.name",
                BsonType.STRING
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for the index to be built and verify both index entries and cardinality
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);

                // Check index entries
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<String> indexValues = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    indexValues.add(unpacked.getString(1));
                }

                // Should have 6 unique entries total (2 + 3 + 1)
                if (indexValues.size() != 6) {
                    return false;
                }

                // Verify all expected values are present
                List<String> expectedValues = List.of("java", "kotlin", "python", "rust", "go", "scala");
                for (String expected : expectedValues) {
                    if (!indexValues.contains(expected)) {
                        return false;
                    }
                }

                // Check cardinality
                var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
                IndexStatistics stats = indexStatistics.get(definition.id());
                return stats != null && stats.cardinality() == 6L;
            }
        });
    }

    @Test
    void shouldBuildObjectIdIndexAtBackground() {
        // Behavior: Creating an OBJECT_ID index after documents exist triggers the background index building
        // that stores ObjectId values as byte[] in both index entries and back pointers.
        ObjectId refId1 = new ObjectId();
        ObjectId refId2 = new ObjectId();

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"ref_id\": {\"$oid\": \"" + refId1.toHexString() + "\"}}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"ref_id\": {\"$oid\": \"" + refId2.toHexString() + "\"}}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<ObjectId> expectedObjectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(2, expectedObjectIds.size());

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "ref-id-index",
                "ref_id",
                BsonType.OBJECT_ID
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<byte[]> expectedIndexValues = new ArrayList<>(List.of(refId1.toByteArray(), refId2.toByteArray()));
            List<byte[]> indexValues = new ArrayList<>();
            List<ObjectId> objectIds = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    // Back pointer key: (BACK_POINTER, ObjectId bytes, indexValue as byte[])
                    byte[] objectIdBytes = unpacked.getBytes(1);
                    objectIds.add(new ObjectId(objectIdBytes));
                    indexValues.add(unpacked.getBytes(2));
                }
                if (!expectedObjectIds.equals(objectIds)) {
                    return false;
                }
                if (indexValues.size() != expectedIndexValues.size()) {
                    return false;
                }
                for (int i = 0; i < indexValues.size(); i++) {
                    if (!Arrays.equals(indexValues.get(i), expectedIndexValues.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        });
    }

    @Test
    void shouldBuildMultikeyObjectIdIndexAtBackground() {
        // Behavior: Background index building on documents with arrays of ObjectId values
        // creates deduplicated index entries with ObjectId stored as byte[].
        ObjectId ref1 = new ObjectId();
        ObjectId ref2 = new ObjectId();
        ObjectId ref3 = new ObjectId();

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"refs\": [{\"$oid\": \"" + ref1.toHexString() + "\"}, {\"$oid\": \"" + ref2.toHexString() + "\"}]}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"refs\": [{\"$oid\": \"" + ref2.toHexString() + "\"}, {\"$oid\": \"" + ref3.toHexString() + "\"}]}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "refs-index",
                "refs",
                BsonType.OBJECT_ID
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Expected unique ObjectId byte arrays sorted for comparison
        List<byte[]> expectedBytes = List.of(ref1.toByteArray(), ref2.toByteArray(), ref3.toByteArray());

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<byte[]> indexValues = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    indexValues.add(unpacked.getBytes(1));
                }
                // Doc1 has ref1, ref2. Doc2 has ref2, ref3. Total unique: ref1, ref2, ref3.
                // But ref2 appears in both docs, so we get 4 entries (2 per doc, not deduplicated across docs).
                if (indexValues.size() != 4) {
                    return false;
                }
                // Verify all expected ObjectId byte arrays are present
                for (byte[] expected : expectedBytes) {
                    boolean found = false;
                    for (byte[] actual : indexValues) {
                        if (Arrays.equals(expected, actual)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        return false;
                    }
                }
                return true;
            }
        });
    }

    @Test
    void shouldIndexNullElementsInMultikeyArrayDuringBackgroundBuild() {
        // Behavior: Background index building on documents with arrays containing null elements
        // should create ONE deduplicated null index entry along with non-null values.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"java\", null, \"kotlin\"]}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Create a multikey index on "tags"
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "tags-index",
                "tags",
                BsonType.STRING
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for the index to be built and verify entries include null
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<Object> indexValues = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    Object value = unpacked.get(1);
                    indexValues.add(value);
                }
                // Should have 3 entries: "java", "kotlin", and null
                return indexValues.size() == 3
                        && indexValues.contains("java")
                        && indexValues.contains("kotlin")
                        && indexValues.contains(null);
            }
        });
    }

    // --- Numeric Widening Integration Tests ---

    @Test
    void shouldWidenInt32ValuesToInt64IndexDuringBackgroundBuild() {
        // Behavior: When documents with INT32 values exist before an INT64 index is created,
        // the background index builder widens INT32 to INT64 and stores Long values in the index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 32}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 40}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<ObjectId> expectedObjectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(2, expectedObjectIds.size());

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "age-int64-index",
                "age",
                BsonType.INT64
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<Long> expectedIndexValues = new ArrayList<>(List.of(32L, 40L));
            List<Long> indexValues = new ArrayList<>();
            List<ObjectId> objectIds = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    byte[] objectIdBytes = unpacked.getBytes(1);
                    objectIds.add(new ObjectId(objectIdBytes));
                    indexValues.add(unpacked.getLong(2));
                }
                return expectedObjectIds.equals(objectIds)
                        && expectedIndexValues.equals(indexValues);
            }
        });
    }

    @Test
    void shouldWidenInt32ValuesToDoubleIndexDuringBackgroundBuild() {
        // Behavior: When documents with INT32 values exist before a DOUBLE index is created,
        // the background index builder widens INT32 to DOUBLE and stores Double values in the index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"score\": 42}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"score\": 99}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "score-double-index",
                "score",
                BsonType.DOUBLE
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<Double> expectedIndexValues = new ArrayList<>(List.of(42.0, 99.0));
            List<Double> indexValues = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    Object value = unpacked.get(1);
                    assertInstanceOf(Double.class, value, "Widened INT32 should be stored as Double");
                    indexValues.add((Double) value);
                }
                indexValues.sort(Double::compareTo);
                return expectedIndexValues.equals(indexValues);
            }
        });
    }

    @Test
    void shouldFailBackgroundBuildWhenInt64ValueMeetsDoubleIndex() {
        // Behavior: INT64 to DOUBLE widening is forbidden (lossy). The background index builder
        // fails the build task with an IndexTypeMismatchException.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"value\": {\"$numberLong\": \"999999999999\"}}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "value-double-index",
                "value",
                BsonType.DOUBLE
                , false, IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        Versionstamp taskId = TestUtil.findIndexMaintenanceTaskId(context, taskSubspace, IndexMaintenanceTaskKind.BUILD);

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
                return state.status() == IndexTaskStatus.FAILED;
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(IndexTaskStatus.FAILED, state.status());
            assertNotNull(state.error(), "Error message should be set");
            assertTrue(state.error().contains("Index type mismatch"),
                    "Error message should indicate type mismatch");
            assertTrue(state.error().contains("value-double-index"),
                    "Error message should mention the index name");
            assertTrue(state.error().contains("DOUBLE"),
                    "Error message should mention expected type");
            assertTrue(state.error().contains("INT64"),
                    "Error message should mention actual type");
        }
    }

    @Test
    void shouldWidenInt32ArrayValuesToInt64MultikeyIndexDuringBackgroundBuild() {
        // Behavior: Each INT32 element in an array is independently widened to INT64 during
        // multikey background index building.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"scores\": [10, 20, 30]}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"scores\": [40, 50]}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "scores-int64-index",
                "scores",
                BsonType.INT64
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<Long> expectedIndexValues = List.of(10L, 20L, 30L, 40L, 50L);
            List<Long> indexValues = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    indexValues.add(unpacked.getLong(1));
                }
                indexValues.sort(Long::compareTo);
                return expectedIndexValues.equals(indexValues);
            }
        });
    }

    @Test
    void shouldTrackCardinalityForWidenedValuesDuringBackgroundBuild() {
        // Behavior: Background index building correctly tracks cardinality for widened INT32 values
        // stored in an INT64 index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 20}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 30}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 40}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "age-int64-cardinality-index",
                "age",
                BsonType.INT64
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
                IndexStatistics stats = indexStatistics.get(definition.id());
                return stats != null && stats.cardinality() == 3L;
            }
        });
    }

    @Test
    void shouldWidenMixedInt32AndInt64ValuesToInt64IndexDuringBackgroundBuild() {
        // Behavior: When documents contain a mix of INT32 and INT64 values in the same field,
        // the background index builder widens all values to INT64 and stores Long values in the index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 25}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": {\"$numberLong\": \"3000000000\"}}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 40}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<ObjectId> expectedObjectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(3, expectedObjectIds.size());

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "age-mixed-int64-index",
                "age",
                BsonType.INT64
                , false, IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<Long> expectedIndexValues = new ArrayList<>(List.of(25L, 3000000000L, 40L));
            List<Long> indexValues = new ArrayList<>();
            List<ObjectId> objectIds = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    byte[] objectIdBytes = unpacked.getBytes(1);
                    objectIds.add(new ObjectId(objectIdBytes));
                    indexValues.add(unpacked.getLong(2));
                }
                return expectedObjectIds.equals(objectIds)
                        && expectedIndexValues.equals(indexValues);
            }
        });
    }

    @Test
    void shouldFailBackgroundBuildWhenMixedInt32AndInt64ValuesMeetDoubleIndex() {
        // Behavior: When documents contain mixed INT32 and INT64 values and a DOUBLE index is created,
        // the builder fails because INT64 to DOUBLE widening is forbidden (lossy), even though
        // INT32 to DOUBLE widening alone would succeed.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"value\": 42}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"value\": {\"$numberLong\": \"999999999999\"}}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "value-mixed-double-index",
                "value",
                BsonType.DOUBLE
                , false, IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        Versionstamp taskId = TestUtil.findIndexMaintenanceTaskId(context, taskSubspace, IndexMaintenanceTaskKind.BUILD);

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
                return state.status() == IndexTaskStatus.FAILED;
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(IndexTaskStatus.FAILED, state.status());
            assertNotNull(state.error(), "Error message should be set");
            assertTrue(state.error().contains("Index type mismatch"),
                    "Error message should indicate type mismatch");
            assertTrue(state.error().contains("value-mixed-double-index"),
                    "Error message should mention the index name");
            assertTrue(state.error().contains("DOUBLE"),
                    "Error message should mention expected type");
            assertTrue(state.error().contains("INT64"),
                    "Error message should mention actual type");
        }
    }

    @Test
    void shouldBuildCollatedIndexAtBackground() {
        // Behavior: Background building of a collated single-field STRING index stores collation key
        // bytes instead of raw strings. Turkish PRIMARY: "istanbul" and "İstanbul" produce identical
        // collation keys. If collation is broken, raw strings are stored (different bytes) and the
        // assertion fails.

        final String BUCKET_NAME = "test-collated-background-build";

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf createBuf = Unpooled.buffer();
        cmd.create(BUCKET_NAME, BucketCreateArgs.Builder
                .collation("{\"locale\": \"tr\", \"strength\": 1}")
                .shards(List.of(SHARD_ID))
                .ifNotExists()).encode(createBuf);
        Object createResponse = runCommand(channel, createBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, createResponse);

        ByteBuf insertBuf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u0130stanbul\"}")
        ));
        cmd.insert(BUCKET_NAME, docs).encode(insertBuf);
        Object insertResponse = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertResponse);

        Collation turkishPrimary = Collation.create("tr", 1, null, null, null, null, null, null, null);
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "name-collated-index", "name", BsonType.STRING, false, IndexStatus.WAITING, turkishPrimary);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, BUCKET_NAME, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, BUCKET_NAME);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                if (index == null) return false;

                SingleFieldIndexDefinition loadedDef = SingleFieldIndexUtil.loadIndexDefinition(tr, index.subspace());
                if (loadedDef.status() != IndexStatus.READY) return false;

                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<KeyValue> entries = tr.getRange(begin, end).asList().join();

                if (entries.size() != 2) return false;

                Tuple entry0 = index.subspace().unpack(entries.get(0).getKey());
                Tuple entry1 = index.subspace().unpack(entries.get(1).getKey());

                byte[] collationKey0 = entry0.getBytes(1);
                byte[] collationKey1 = entry1.getBytes(1);

                assertArrayEquals(collationKey0, collationKey1,
                        "Turkish PRIMARY: 'istanbul' and 'İstanbul' must produce identical collation key bytes");
                return true;
            }
        });
    }
}