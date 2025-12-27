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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FullScanNodeDeleteTest extends BasePipelineTest {

    @Test
    void shouldDeleteWithGreaterThanFilter() {
        final String TEST_BUCKET_NAME = "test-bucket-gt-delete-full-scan";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Versionstamp> results = deleteExecutor.execute(tr, ctx);

            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22");
            tr.commit().join();
        }

        // age > 22 deleted
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(0, results.size());
        }
    }

    @Test
    void shouldDeleteWithLimitedBatchAndPagination() {
        final String TEST_BUCKET_NAME = "test-bucket-batch-delete";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 21, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 36, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 37, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 45, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 56, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 75, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 95, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
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
    }
}
