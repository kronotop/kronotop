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

class CompoundIndexBuildingRoutineTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldBuildCompoundIndexAtBackground() {
        // Behavior: Creating a compound index after documents exist triggers background building
        // that populates entries and back pointers for all existing documents.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 32}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 40}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<ObjectId> expectedObjectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(2, expectedObjectIds.size());

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name-age-index",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<ObjectId> objectIds = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = compoundIndex.subspace().unpack(entry.getKey());
                    // Back pointer key: (BACK_POINTER, ObjectId bytes, name, age)
                    byte[] objectIdBytes = unpacked.getBytes(1);
                    objectIds.add(new ObjectId(objectIdBytes));
                }
                return expectedObjectIds.equals(objectIds);
            }
        });

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
    }

    @Test
    void shouldMarkCompoundIndexReadyForEmptyBucket() {
        // Behavior: Creating a compound index on an empty bucket immediately marks it READY
        // without spawning building tasks.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name-age-index",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return compoundIndex != null && compoundIndex.definition().status() == IndexStatus.READY;
        });
    }

    @Test
    void shouldFailCompoundIndexBuildingWhenTypeMismatchOccurs() {
        // Behavior: Background compound index building fails with a type mismatch error when
        // document field types don't match the index definition.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": \"thirty-two\"}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": \"forty\"}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name-age-index",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
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
            assertTrue(state.error().contains("name-age-index"),
                    "Error message should mention the index name");
            assertTrue(state.error().contains("INT32"),
                    "Error message should mention expected type");
            assertTrue(state.error().contains("STRING"),
                    "Error message should mention actual type");
        }
    }

    @Test
    void shouldBuildCompoundIndexWithMultikeyField() {
        // Behavior: Background compound index building correctly handles array fields, creating
        // one entry per unique array element combined with the other field values.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"java\", \"kotlin\"], \"priority\": 1}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"python\"], \"priority\": 2}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "tags-priority-index",
                List.of(
                        new CompoundIndexField("tags", BsonType.STRING, true),
                        new CompoundIndexField("priority", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Expect 3 entries: (java, 1), (kotlin, 1), (python, 2)
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<String> tagValues = new ArrayList<>();
                List<Long> priorityValues = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = compoundIndex.subspace().unpack(entry.getKey());
                    // Entry key: (ENTRIES, tag, priority, ObjectId)
                    tagValues.add(unpacked.getString(1));
                    priorityValues.add(unpacked.getLong(2));
                }
                if (tagValues.size() != 3) {
                    return false;
                }
                // Entries are sorted by first field (tag), then second field (priority)
                // Expected: (java, 1), (kotlin, 1), (python, 2)
                return tagValues.get(0).equals("java") && priorityValues.get(0) == 1L
                        && tagValues.get(1).equals("kotlin") && priorityValues.get(1) == 1L
                        && tagValues.get(2).equals("python") && priorityValues.get(2) == 2L;
            }
        });
    }

    @Test
    void shouldDeduplicateMultikeyEntriesInCompoundIndex() {
        // Behavior: Array fields with duplicate values should produce only unique combinations
        // in the compound index entries.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"java\", \"java\", \"kotlin\"], \"priority\": 1}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "tags-priority-dedup-index",
                List.of(
                        new CompoundIndexField("tags", BsonType.STRING, true),
                        new CompoundIndexField("priority", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Expect 2 entries: (java, 1) and (kotlin, 1), not 3
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<String> tagValues = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = compoundIndex.subspace().unpack(entry.getKey());
                    tagValues.add(unpacked.getString(1));
                }
                return tagValues.size() == 2
                        && tagValues.contains("java")
                        && tagValues.contains("kotlin");
            }
        });
    }

    @Test
    void shouldBuildCompoundIndexWithObjectIdField() {
        // Behavior: Compound index building correctly stores ObjectId values as byte[] in index entries.
        ObjectId refId1 = new ObjectId();
        ObjectId refId2 = new ObjectId();

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"ref_id\": {\"$oid\": \"" + refId1.toHexString() + "\"}, \"status\": \"active\"}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"ref_id\": {\"$oid\": \"" + refId2.toHexString() + "\"}, \"status\": \"inactive\"}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<ObjectId> expectedObjectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(2, expectedObjectIds.size());

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "refid-status-index",
                List.of(
                        new CompoundIndexField("ref_id", BsonType.OBJECT_ID, false),
                        new CompoundIndexField("status", BsonType.STRING, false)
                )
                , IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<ObjectId> objectIds = new ArrayList<>();
                List<byte[]> refIdValues = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = compoundIndex.subspace().unpack(entry.getKey());
                    byte[] objectIdBytes = unpacked.getBytes(1);
                    objectIds.add(new ObjectId(objectIdBytes));
                    refIdValues.add(unpacked.getBytes(2));
                }
                if (!expectedObjectIds.equals(objectIds)) {
                    return false;
                }
                List<byte[]> expectedRefIds = List.of(refId1.toByteArray(), refId2.toByteArray());
                if (refIdValues.size() != expectedRefIds.size()) {
                    return false;
                }
                for (int i = 0; i < refIdValues.size(); i++) {
                    if (!Arrays.equals(refIdValues.get(i), expectedRefIds.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        });
    }

    @Test
    void shouldIndexDocumentsWithMissingFieldsAsNullInCompoundIndex() {
        // Behavior: Documents missing one or more indexed fields store null at those positions
        // in the compound index entries, rather than being skipped.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name-age-missing-index",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Both documents should be indexed: Alice with (Alice, 30), Bob with (Bob, null)
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<String> names = new ArrayList<>();
                List<Object> ages = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = compoundIndex.subspace().unpack(entry.getKey());
                    names.add(unpacked.getString(1));
                    ages.add(unpacked.get(2));
                }
                // Entries sorted by name: (Alice, 30), (Bob, null)
                return names.size() == 2
                        && names.get(0).equals("Alice") && ages.get(0).equals(30L)
                        && names.get(1).equals("Bob") && ages.get(1) == null;
            }
        });
    }

    @Test
    void shouldBuildCompoundIndexWithThreeFields() {
        // Behavior: Compound index building works correctly with more than 2 fields.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"city\": \"Istanbul\", \"district\": \"Kadikoy\", \"zip\": 34710}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"city\": \"Ankara\", \"district\": \"Cankaya\", \"zip\": 6690}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "city-district-zip-index",
                List.of(
                        new CompoundIndexField("city", BsonType.STRING, false),
                        new CompoundIndexField("district", BsonType.STRING, false),
                        new CompoundIndexField("zip", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<String> cities = new ArrayList<>();
                List<String> districts = new ArrayList<>();
                List<Long> zips = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = compoundIndex.subspace().unpack(entry.getKey());
                    cities.add(unpacked.getString(1));
                    districts.add(unpacked.getString(2));
                    zips.add(unpacked.getLong(3));
                }
                if (cities.size() != 2) {
                    return false;
                }
                // Entries are sorted by first field
                return cities.get(0).equals("Ankara")
                        && districts.get(0).equals("Cankaya")
                        && zips.get(0) == 6690L
                        && cities.get(1).equals("Istanbul")
                        && districts.get(1).equals("Kadikoy")
                        && zips.get(1) == 34710L;
            }
        });
    }

    @Test
    void shouldCleanupTasksAfterCompoundIndexBuildCompletes() {
        // Behavior: After compound index building completes, the maintenance task sweeper cleans up
        // all tasks and the index transitions to READY status.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"x\": 1, \"y\": 2}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"x\": 3, \"y\": 4}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"x\": 5, \"y\": 6}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "x-y-index",
                List.of(
                        new CompoundIndexField("x", BsonType.INT32, false),
                        new CompoundIndexField("y", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for READY status
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return compoundIndex != null && compoundIndex.definition().status() == IndexStatus.READY;
        });

        // Wait for task cleanup
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
    }

    @Test
    void shouldBuildCompoundIndexWithNullArrayElements() {
        // Behavior: Array fields containing null elements in compound index produce one deduplicated
        // null entry alongside non-null entries.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"java\", null, \"kotlin\"], \"priority\": 1}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "tags-priority-null-index",
                List.of(
                        new CompoundIndexField("tags", BsonType.STRING, true),
                        new CompoundIndexField("priority", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Expect 3 entries: (java, 1), (kotlin, 1), (null, 1)
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<Object> tagValues = new ArrayList<>();
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = compoundIndex.subspace().unpack(entry.getKey());
                    tagValues.add(unpacked.get(1));
                }
                return tagValues.size() == 3
                        && tagValues.contains("java")
                        && tagValues.contains("kotlin")
                        && tagValues.contains(null);
            }
        });
    }

    // --- Numeric Widening Integration Tests ---

    @Test
    void shouldWidenInt32FieldToInt64InCompoundIndexDuringBackgroundBuild() {
        // Behavior: In a compound index, an INT64 field correctly receives widened INT32 document
        // values during background build.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 32}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 40}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<ObjectId> expectedObjectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(2, expectedObjectIds.size());

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name-age-int64-index",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT64, false)
                )
                , IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<ObjectId> objectIds = new ArrayList<>();
            List<String> names = new ArrayList<>();
            List<Long> ages = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = compoundIndex.subspace().unpack(entry.getKey());
                    byte[] objectIdBytes = unpacked.getBytes(1);
                    objectIds.add(new ObjectId(objectIdBytes));
                    names.add(unpacked.getString(2));
                    ages.add(unpacked.getLong(3));
                }
                return expectedObjectIds.equals(objectIds)
                        && names.equals(List.of("Alice", "Bob"))
                        && ages.equals(List.of(32L, 40L));
            }
        });
    }

    @Test
    void shouldWidenMultipleFieldsInCompoundIndexDuringBackgroundBuild() {
        // Behavior: Multiple numeric fields in a compound index are independently widened from
        // INT32 to INT64 during background build.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"x\": 10, \"y\": 20}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"x\": 30, \"y\": 40}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "x-y-int64-index",
                List.of(
                        new CompoundIndexField("x", BsonType.INT64, false),
                        new CompoundIndexField("y", BsonType.INT64, false)
                )
                , IndexStatus.WAITING);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            List<Long> xValues = new ArrayList<>();
            List<Long> yValues = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = compoundIndex.subspace().unpack(entry.getKey());
                    xValues.add(unpacked.getLong(1));
                    yValues.add(unpacked.getLong(2));
                }
                if (xValues.size() != 2) {
                    return false;
                }
                return xValues.get(0) == 10L && yValues.get(0) == 20L
                        && xValues.get(1) == 30L && yValues.get(1) == 40L;
            }
        });
    }

    @Test
    void shouldBuildCollatedCompoundIndexAtBackground() {
        // Behavior: Background building of a collated compound index stores collation key bytes for
        // STRING fields. French PRIMARY: "eclair" and "Éclair" produce identical collation keys.
        // If collation is broken, raw strings are stored (different bytes) and the assertion fails.

        final String BUCKET_NAME = "test-collated-compound-background";

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf createBuf = Unpooled.buffer();
        cmd.create(BUCKET_NAME, BucketCreateArgs.Builder
                .collation("{\"locale\": \"fr\", \"strength\": 1}")
                .shards(List.of(SHARD_ID))
                .ifNotExists()).encode(createBuf);
        Object createResponse = runCommand(channel, createBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, createResponse);

        ByteBuf insertBuf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"eclair\", \"age\": 32}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u00c9clair\", \"age\": 40}")
        ));
        cmd.insert(BUCKET_NAME, docs).encode(insertBuf);
        Object insertResponse = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertResponse);

        Collation frenchPrimary = Collation.create("fr", 1, null, null, null, null, null, null, null);
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name-age-collated-index",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                ), IndexStatus.WAITING, frenchPrimary);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, BUCKET_NAME, definition);
            tr.commit().join();
        }

        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, BUCKET_NAME);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                if (compoundIndex == null) return false;

                CompoundIndexDefinition loadedDef = CompoundIndexUtil.loadIndexDefinition(tr, compoundIndex.subspace());
                if (loadedDef.status() != IndexStatus.READY) return false;

                byte[] begin = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                List<KeyValue> entries = tr.getRange(begin, end).asList().join();

                if (entries.size() != 2) return false;

                Tuple entry0 = compoundIndex.subspace().unpack(entries.get(0).getKey());
                Tuple entry1 = compoundIndex.subspace().unpack(entries.get(1).getKey());

                byte[] nameKey0 = entry0.getBytes(1);
                byte[] nameKey1 = entry1.getBytes(1);

                assertArrayEquals(nameKey0, nameKey1,
                        "French PRIMARY: 'eclair' and '\u00c9clair' must produce identical collation key bytes");

                long age0 = entry0.getLong(2);
                long age1 = entry1.getLong(2);
                assertTrue((age0 == 32L && age1 == 40L) || (age0 == 40L && age1 == 32L),
                        "Age fields should be stored as integers, not affected by collation");

                return true;
            }
        });
    }
}
