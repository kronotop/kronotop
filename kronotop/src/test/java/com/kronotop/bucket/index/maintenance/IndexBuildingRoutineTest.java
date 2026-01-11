/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class IndexBuildingRoutineTest extends BaseBucketHandlerTest {
    @Test
    void shouldBuildIndexAtBackground() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 32}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 40}")
                ));
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<Versionstamp> expectedVersionstamps = new ArrayList<>();
        assertEquals(2, actualMessage.children().size());
        for (int i = 0; i < actualMessage.children().size(); i++) {
            SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().get(i);
            assertNotNull(message.content());
            expectedVersionstamps.add(VersionstampUtil.base32HexDecode(message.content()));
        }

        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "age",
                BsonType.INT32
        );

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            List<Long> expectedIndexValues = new ArrayList<>(List.of(32L, 40L));
            List<Long> indexValues = new ArrayList<>();
            List<Versionstamp> versionstamps = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    indexValues.add(unpacked.getLong(2));
                    versionstamps.add((Versionstamp) unpacked.get(1));
                }
                return expectedVersionstamps.equals(versionstamps)
                        && expectedIndexValues.equals(indexValues);
            }
        });

        await().atMost(15, TimeUnit.SECONDS).until(() -> {
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
    void shouldFailIndexBuildingWhenTypeMismatchOccurs() {
        // Insert documents with STRING values for the 'age' field
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": \"thirty-two\"}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": \"forty\"}")
                ));
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        // Create an index expecting INT32 for 'age' field
        IndexDefinition definition = IndexDefinition.create(
                "age-index",
                "age",
                BsonType.INT32
        );

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        Versionstamp taskId = TestUtil.findIndexMaintenanceTaskId(context, taskSubspace, IndexMaintenanceTaskKind.BUILD);

        // Wait for the task to fail due to type mismatch
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
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
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        // Create a multikey index on "scores.type"
        IndexDefinition definition = IndexDefinition.create(
                "scores-type-index",
                "scores.type",
                BsonType.STRING
        );

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for the index to be built and verify entries
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
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
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Create a multikey index on "tags.name"
        IndexDefinition definition = IndexDefinition.create(
                "tags-name-index",
                "tags.name",
                BsonType.STRING
        );

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for index to be built and verify only unique entries exist
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
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
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Create a multikey index on "items.color"
        IndexDefinition definition = IndexDefinition.create(
                "items-color-index",
                "items.color",
                BsonType.STRING
        );

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for the index to be built and verify cardinality
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
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
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Create a multikey index on "langs.name"
        IndexDefinition definition = IndexDefinition.create(
                "langs-name-index",
                "langs.name",
                BsonType.STRING
        );

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for index to be built and verify both index entries and cardinality
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
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
}