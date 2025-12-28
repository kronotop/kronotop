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
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class IntersectionNodeWithIndexScanStrategyTest extends BasePipelineTest {
    @Test
    void shouldIntersectWithTwoFieldsAndSingleIndex() {
        final String TEST_BUCKET_NAME = "test-intersection-with-two-fields-and-single-index";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size(), "Should return exactly 3 documents with age > 22");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldIntersectWithBatchedIterationAndLimit() {
        final String TEST_BUCKET_NAME = "test-intersection-batched-iteration";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 20 documents with 7 matching the query criteria
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),    // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 21, 'name': 'John'}"),     // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 22, 'name': 'Bob'}"),      // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'John'}"),     // match 1
                BSONUtil.jsonToDocumentThenBytes("{'age': 24, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'John'}"),     // match 2
                BSONUtil.jsonToDocumentThenBytes("{'age': 26, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 27, 'name': 'John'}"),     // match 3
                BSONUtil.jsonToDocumentThenBytes("{'age': 28, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 29, 'name': 'John'}"),     // match 4
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 31, 'name': 'John'}"),     // match 5
                BSONUtil.jsonToDocumentThenBytes("{'age': 32, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 33, 'name': 'John'}"),     // match 6
                BSONUtil.jsonToDocumentThenBytes("{'age': 34, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"),     // match 7
                BSONUtil.jsonToDocumentThenBytes("{'age': 36, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 37, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 38, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 39, 'name': 'Bob'}")       // no match (name != John)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        // Expected ages for matching documents
        List<Integer> expectedAges = List.of(23, 25, 27, 29, 31, 33, 35);
        int totalMatches = expectedAges.size();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Batch 1: Should return 2 results
            Map<?, ByteBuffer> batch1 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch1.size());
            assertEquals(Set.of(23, 25), extractIntegerFieldFromResults(batch1, "age"));

            // Batch 2: Should return 2 results
            Map<?, ByteBuffer> batch2 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch2.size());
            assertEquals(Set.of(27, 29), extractIntegerFieldFromResults(batch2, "age"));

            // Batch 3: Should return 2 results
            Map<?, ByteBuffer> batch3 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch3.size());
            assertEquals(Set.of(31, 33), extractIntegerFieldFromResults(batch3, "age"));

            // Batch 4: Should return 1 result (last match)
            Map<?, ByteBuffer> batch4 = readExecutor.execute(tr, ctx);
            assertEquals(1, batch4.size());
            assertEquals(Set.of(35), extractIntegerFieldFromResults(batch4, "age"));

            // Batch 5: Should be empty
            Map<?, ByteBuffer> batch5 = readExecutor.execute(tr, ctx);
            assertEquals(0, batch5.size());
        }
    }

    @Test
    void shouldIntersectWithBatchedIterationAndLimitReverse() {
        final String TEST_BUCKET_NAME = "test-intersection-batched-iteration-reverse";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 20 documents with 7 matching the query criteria
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),    // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 21, 'name': 'John'}"),     // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 22, 'name': 'Bob'}"),      // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'John'}"),     // match 1
                BSONUtil.jsonToDocumentThenBytes("{'age': 24, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'John'}"),     // match 2
                BSONUtil.jsonToDocumentThenBytes("{'age': 26, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 27, 'name': 'John'}"),     // match 3
                BSONUtil.jsonToDocumentThenBytes("{'age': 28, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 29, 'name': 'John'}"),     // match 4
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 31, 'name': 'John'}"),     // match 5
                BSONUtil.jsonToDocumentThenBytes("{'age': 32, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 33, 'name': 'John'}"),     // match 6
                BSONUtil.jsonToDocumentThenBytes("{'age': 34, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"),     // match 7
                BSONUtil.jsonToDocumentThenBytes("{'age': 36, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 37, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 38, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 39, 'name': 'Bob'}")       // no match (name != John)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().limit(2).reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Batch 1: Should return 2 results (highest ages first)
            Map<?, ByteBuffer> batch1 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch1.size());
            assertEquals(Set.of(35, 33), extractIntegerFieldFromResults(batch1, "age"));

            // Batch 2: Should return 2 results
            Map<?, ByteBuffer> batch2 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch2.size());
            assertEquals(Set.of(31, 29), extractIntegerFieldFromResults(batch2, "age"));

            // Batch 3: Should return 2 results
            Map<?, ByteBuffer> batch3 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch3.size());
            assertEquals(Set.of(27, 25), extractIntegerFieldFromResults(batch3, "age"));

            // Batch 4: Should return 1 result (last match)
            Map<?, ByteBuffer> batch4 = readExecutor.execute(tr, ctx);
            assertEquals(1, batch4.size());
            assertEquals(Set.of(23), extractIntegerFieldFromResults(batch4, "age"));

            // Batch 5: Should be empty
            Map<?, ByteBuffer> batch5 = readExecutor.execute(tr, ctx);
            assertEquals(0, batch5.size());
        }
    }

    @Test
    void shouldIntersectWithTwoFieldsAndDoubleIndex() {
        final String TEST_BUCKET_NAME = "test-intersection-with-two-fields-and-double-index";

        IndexDefinition nameIndex = IndexDefinition.create("name-index", "name", BsonType.STRING);
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size(), "Should return exactly 3 documents with age > 22");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldIntersectWithRangeScanAndDoubleIndex() {
        final String TEST_BUCKET_NAME = "test-intersection-with-two-fields-and-double-index";

        IndexDefinition nameIndex = IndexDefinition.create("name-index", "name", BsonType.STRING);
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gt': 22, '$lt': 50 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size(), "Should return exactly 3 documents with age > 22");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldExecuteAndQueryWithTwoEqPredicates() {
        // Create two secondary indexes
        IndexDefinition ageIndex = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexDefinition nameIndex = IndexDefinition.create("name_idx", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, ageIndex, nameIndex);

        // Insert documents with both fields
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 25}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Create an execution plan with $and on two indexed fields
        String query = "{ $and: [ { age: { $eq: 25 } }, { name: { $eq: \"Alice\" } } ] }";
        PipelineNode plan = createExecutionPlan(metadata, query);

        assertNotNull(plan);

        // Run the query and verify the result
        List<String> results = runQueryOnBucket(metadata, query);
        assertEquals(1, results.size());

        // Verify the returned document is Alice with age 25
        String resultJson = results.getFirst();
        assertTrue(resultJson.contains("\"name\": \"Alice\""));
        assertTrue(resultJson.contains("\"age\": 25"));
    }
}
