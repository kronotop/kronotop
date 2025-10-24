/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RangeScanNodeDeleteTest extends BasePipelineTest {

    @Test
    void testDeleteWithRangeFilter() {
        final String TEST_BUCKET_NAME = "test-bucket-range-delete";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gte': 23, '$lte': 30}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext deleteCtx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Versionstamp> results = deleteExecutor.execute(tr, deleteCtx);
            assertEquals(3, results.size(), "Should return exactly 3 documents with age between 23 and 30");
            tr.commit().join();
        }

        QueryContext readCtx = new QueryContext(metadata, config, plan);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, readCtx);
            assertEquals(0, results.size());
        }

        Index index = metadata.indexes().getIndex(ageIndex.selector(), IndexSelectionPolicy.READONLY);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        List<KeyValue> entries = fetchAllIndexedEntries(indexSubspace);
        assertEquals(2, entries.size());

        List<KeyValue> backPointers = fetchAllIndexBackPointers(indexSubspace);
        assertEquals(2, backPointers.size());
    }

    @Test
    void testLimitedBatchDeleteWithPagination() {
        final String TEST_BUCKET_NAME = "test-bucket-range-batch-delete";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 32, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 38, 'name': 'David'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Emma'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 42, 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 45, 'name': 'Grace'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'Henry'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 49, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 50, 'name': 'Ivy'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 55, 'name': 'Jack'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 60, 'name': 'Kate'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gte': 25, '$lte': 50}}");
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext deleteCtx = new QueryContext(metadata, config, plan);

        int expectedBatchCount = 6;
        int iterationCount = 0;

        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                iterationCount++;
                List<Versionstamp> results = deleteExecutor.execute(tr, deleteCtx);
                if (results.isEmpty()) {
                    break;
                }

                int expectedBatchSize = (iterationCount < expectedBatchCount) ? 2 : 1;
                assertEquals(expectedBatchSize, results.size());
                tr.commit().join();
            }
        }
        assertEquals(7, iterationCount);

        QueryContext readCtx = new QueryContext(metadata, config, plan);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, readCtx);
            assertEquals(0, results.size());
        }

        Index index = metadata.indexes().getIndex(ageIndex.selector(), IndexSelectionPolicy.READONLY);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        List<KeyValue> entries = fetchAllIndexedEntries(indexSubspace);
        assertEquals(3, entries.size());

        List<KeyValue> backPointers = fetchAllIndexBackPointers(indexSubspace);
        assertEquals(3, backPointers.size());
    }
}
