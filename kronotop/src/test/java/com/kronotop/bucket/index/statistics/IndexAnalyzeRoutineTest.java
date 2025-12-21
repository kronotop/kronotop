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
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class IndexAnalyzeRoutineTest extends BaseBucketHandlerTest {

    @Test
    void shouldBuildHistogramWhenNoHintFound() {
        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "numeric",
                BsonType.INT32,
                IndexStatus.WAITING
        );

        List<byte[]> documents = generateRandomDocumentsWithNumericContent("numeric", 1000);
        insertDocuments(documents, 50);

        createIndexThenWaitForReadiness(definition);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, definition.id(), SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(15)).until(() -> {
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
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, 12345, SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return TaskStorage.getDefinition(tr, taskSubspace, taskId) == null;
            }
        });
    }

    @Test
    void shouldIndexAnalyzeTaskFailedBecauseNoSuchIndex() {
        // Insert some documents to create the bucket
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}")
        );
        insertDocuments(documents);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, 12345, SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexAnalyzeTaskState state = IndexAnalyzeTaskState.load(tr, taskSubspace, taskId);
                return state.status() == IndexTaskStatus.FAILED && state.error().equals("No such index");
            }
        });
    }

    @Test
    void shouldBuildHistogramWhenNotReadyToAnalyze() {
        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "numeric",
                BsonType.INT32,
                IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(definition);

        // Index is ready to analyze but change status to BUILDING to trigger the error.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.openUncached(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), definition.name());
            IndexDefinition indexDefinition = IndexUtil.loadIndexDefinition(tr, indexSubspace);
            IndexDefinition updatedDefinition = indexDefinition.updateStatus(IndexStatus.BUILDING);
            IndexUtil.saveIndexDefinition(tr, metadata, updatedDefinition);
            tr.commit().join();
        }

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, definition.id(), SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexAnalyzeTaskState state = IndexAnalyzeTaskState.load(tr, taskSubspace, taskId);
                return state.status() == IndexTaskStatus.FAILED && state.error().equals("Index is not ready to analyze");
            }
        });
    }

    @Test
    void shouldBuildHistogramWithHints() {
        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "numeric",
                BsonType.INT32,
                IndexStatus.WAITING
        );

        List<byte[]> documents = generateRandomDocumentsWithNumericContent("numeric", 1000);
        Map<String, byte[]> items = insertDocuments(documents, 50);

        createIndexThenWaitForReadiness(definition);

        List<String> randomKeys = selectRandomKeysFromMap(items, 200);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.READ);
            assertNotNull(index);
            for (String key : randomKeys) {
                IndexStatsBuilder.insertHintForStats(tr, VersionstampUtil.base32HexDecode(key), index);
            }
            tr.commit().join();
        }

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexAnalyzeTask task = new IndexAnalyzeTask(TEST_NAMESPACE, TEST_BUCKET, definition.id(), SHARD_ID);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(Duration.ofSeconds(15)).until(() -> {
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