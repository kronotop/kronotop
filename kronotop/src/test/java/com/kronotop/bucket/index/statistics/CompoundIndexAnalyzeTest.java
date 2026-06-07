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
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class CompoundIndexAnalyzeTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    private List<byte[]> generateDocumentsWithCategoryAndPrice(int count) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String doc = String.format(
                    "{\"category\": \"cat-%d\", \"price\": %d}",
                    rand.nextInt(100), rand.nextInt(1, 10000)
            );
            result.add(BSONUtil.jsonToDocumentThenBytes(doc));
        }
        return result;
    }

    @Test
    void shouldBuildHistogramForCompoundIndexWhenNoHintFound() {
        // Behavior: IndexAnalyzeRoutine builds a histogram for a compound index using the
        // no-hint path when no stat hints are present. The composite key is treated as BsonBinary.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "cat_price_idx",
                List.of(
                        new CompoundIndexField("category", BsonType.STRING, false),
                        new CompoundIndexField("price", BsonType.INT32, false)
                ),
                IndexStatus.WAITING
        );

        List<byte[]> documents = generateDocumentsWithCategoryAndPrice(1000);
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
            assertNotNull(value, "Histogram should be saved for compound index");
            List<HistogramBucket> histogram = HistogramCodec.decode(value);
            assertFalse(histogram.isEmpty(), "Histogram should have buckets");
            // Compound histogram buckets should contain BsonBinary values
            assertEquals(BsonType.BINARY, histogram.getFirst().min().getBsonType());
        }
    }

    @Test
    void shouldBuildHistogramForCompoundIndexWithHints() {
        // Behavior: IndexAnalyzeRoutine builds a histogram for a compound index using hint-based
        // pivots when stat hints are present in the STAT_HINTS subspace.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "cat_price_idx",
                List.of(
                        new CompoundIndexField("category", BsonType.STRING, false),
                        new CompoundIndexField("price", BsonType.INT32, false)
                ),
                IndexStatus.WAITING
        );

        List<byte[]> documents = generateDocumentsWithCategoryAndPrice(1000);
        Map<ObjectId, byte[]> items = insertDocumentsAndGetObjectIds(documents, 50);

        createIndexThenWaitForReadiness(definition);

        // Manually write hints
        List<ObjectId> randomKeys = selectRandomKeysFromMap(items, 200);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.READ);
            assertNotNull(compoundIndex);
            for (ObjectId objectId : randomKeys) {
                IndexStatsBuilder.setHintForStats(tr, compoundIndex, objectId.toByteArray());
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
            assertNotNull(value, "Histogram should be saved for compound index with hints");
            List<HistogramBucket> histogram = HistogramCodec.decode(value);
            assertFalse(histogram.isEmpty(), "Histogram should have buckets");
        }
    }

    @Test
    void shouldLoadCompoundIndexStatisticsAfterReload() {
        // Behavior: After the histogram is built for a compound index, the statistics should be
        // accessible via CompoundIndexRegistry.getStatistics() after a metadata reload.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "cat_price_idx",
                List.of(
                        new CompoundIndexField("category", BsonType.STRING, false),
                        new CompoundIndexField("price", BsonType.INT32, false)
                ),
                IndexStatus.WAITING
        );

        List<byte[]> documents = generateDocumentsWithCategoryAndPrice(500);
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

        // Reload metadata from scratch - this reads stats via BucketMetadataHeader.read()
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            IndexStatistics stats = metadata.compoundIndexes().getStatistics(definition.id());
            assertNotNull(stats, "Statistics should be loaded for compound index");
            assertNotNull(stats.histogram(), "Histogram should be present");
            assertFalse(stats.histogram().isEmpty(), "Histogram should not be empty");
        }
    }
}
