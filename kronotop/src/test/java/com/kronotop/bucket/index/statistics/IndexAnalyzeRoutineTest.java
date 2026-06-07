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

package com.kronotop.bucket.index.statistics;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.maintenance.IndexTaskStatus;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class IndexAnalyzeRoutineTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldBuildHistogramWhenNoHintFound() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "test-index",
                "numeric",
                BsonType.INT32,
                false,
                IndexStatus.WAITING
        );

        List<byte[]> documents = generateRandomDocumentsWithNumericContent("numeric", 1000);
        insertDocumentsAndGetObjectIds(documents, 50);

        createIndexThenWaitForReadiness(definition);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, definition.id(), SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return TaskStorage.getDefinition(tr, taskSubspace, taskId) == null;
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            byte[] key = IndexUtil.histogramKey(metadata.subspace(), definition.id());
            byte[] value = tr.get(key).join();
            List<HistogramBucket> histogram = HistogramCodec.decode(value);
            assertFalse(histogram.isEmpty());
        }
    }

    @Test
    void shouldIndexAnalyzeTaskFailedBecauseNoSuchBucket() {
        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, "non-existing-bucket", 12345, SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return TaskStorage.getDefinition(tr, taskSubspace, taskId) == null;
            }
        });
    }

    @Test
    void shouldIndexAnalyzeTaskDroppedBecauseNoSuchIndex() {
        // Behavior: When the referenced index does not exist, the watchdog drops the orphaned task
        // during garbage collection before any worker runs.
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}")
        );
        insertDocumentsAndGetObjectIds(documents);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, 12345, SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return TaskStorage.getDefinition(tr, taskSubspace, taskId) == null;
            }
        });
    }

    @Test
    void shouldBuildHistogramWhenNotReadyToAnalyze() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "test-index",
                "numeric",
                BsonType.INT32,
                false,
                IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(definition);

        // Index is ready to analyze but change the status to BUILDING to trigger the error.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), definition.name());
            SingleFieldIndexDefinition indexDefinition = SingleFieldIndexUtil.loadIndexDefinition(tr, indexSubspace);
            SingleFieldIndexDefinition updatedDefinition = indexDefinition.updateStatus(IndexStatus.BUILDING);
            SingleFieldIndexUtil.saveIndexDefinition(tr, metadata, updatedDefinition);
            tr.commit().join();
        }

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, definition.id(), SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexAnalyzeTaskState state = IndexAnalyzeTaskState.load(tr, taskSubspace, taskId);
                return state.status() == IndexTaskStatus.FAILED && state.error().equals("Index is not ready to analyze");
            }
        });
    }

    @Test
    void shouldBuildHistogramForPrimaryIndexWhenNoHintFound() {
        // Behavior: IndexAnalyzeRoutine builds a histogram for the primary index using the
        // no-hint path (buildHistogramFromEntries) when no stat hints are present.
        List<byte[]> documents = generateRandomDocumentsWithNumericContent("numeric", 1000);
        insertDocumentsAndGetObjectIds(documents, 50);

        SingleFieldIndexDefinition definition;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
            assertNotNull(primaryIndex);
            definition = primaryIndex.definition();
        }

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, definition.id(), SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return TaskStorage.getDefinition(tr, taskSubspace, taskId) == null;
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            byte[] key = IndexUtil.histogramKey(metadata.subspace(), definition.id());
            byte[] value = tr.get(key).join();
            List<HistogramBucket> histogram = HistogramCodec.decode(value);
            assertFalse(histogram.isEmpty());
        }
    }

    @Test
    void shouldBuildHistogramForPrimaryIndexWithHints() {
        // Behavior: IndexAnalyzeRoutine builds a histogram for the primary index using hint-based
        // pivots (buildHistogramFromHints + aggregateKeysAroundPivotForPrimaryIndex) when stat
        // hints are present in the STAT_HINTS subspace.
        List<byte[]> documents = generateRandomDocumentsWithNumericContent("numeric", 1000);
        Map<ObjectId, byte[]> items = insertDocumentsAndGetObjectIds(documents, 50);

        SingleFieldIndexDefinition definition;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
            assertNotNull(primaryIndex);
            definition = primaryIndex.definition();
        }

        // Manually write hints using the unconditional overload
        List<ObjectId> randomKeys = selectRandomKeysFromMap(items, 200);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.READ);
            assertNotNull(index);
            for (ObjectId objectId : randomKeys) {
                IndexStatsBuilder.setHintForStats(tr, index, objectId.toByteArray());
            }
            tr.commit().join();
        }

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, definition.id(), SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return TaskStorage.getDefinition(tr, taskSubspace, taskId) == null;
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            byte[] key = IndexUtil.histogramKey(metadata.subspace(), definition.id());
            byte[] value = tr.get(key).join();
            List<HistogramBucket> histogram = HistogramCodec.decode(value);
            assertFalse(histogram.isEmpty());
        }
    }

    @Test
    void shouldBuildHistogramWithHints() {
        // Behavior: IndexAnalyzeRoutine builds a histogram using ObjectId-based hints when hints
        // are present in the STAT_HINTS subspace.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "test-index",
                "numeric",
                BsonType.INT32,
                false,
                IndexStatus.WAITING
        );

        List<byte[]> documents = generateRandomDocumentsWithNumericContent("numeric", 1000);
        Map<ObjectId, byte[]> items = insertDocumentsAndGetObjectIds(documents, 50);

        createIndexThenWaitForReadiness(definition);

        // Use ObjectIds from inserted documents as hints
        List<ObjectId> randomKeys = selectRandomKeysFromMap(items, 200);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.READ);
            assertNotNull(index);
            for (ObjectId objectId : randomKeys) {
                IndexStatsBuilder.setHintForStats(tr, index, objectId.toByteArray());
            }
            tr.commit().join();
        }

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, definition.id(), SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return TaskStorage.getDefinition(tr, taskSubspace, taskId) == null;
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            byte[] key = IndexUtil.histogramKey(metadata.subspace(), definition.id());
            byte[] value = tr.get(key).join();
            List<HistogramBucket> histogram = HistogramCodec.decode(value);
            assertFalse(histogram.isEmpty());
        }
    }
}