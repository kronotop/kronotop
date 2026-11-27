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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.SortDirection;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.index.TestIndexDefinitions;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AndOperatorWithIndexScanStrategyTest extends BasePipelineTest {

    @Test
    void shouldAndQueryWithTwoFieldsAndSingleIndex() {
        // Behavior: AND query with one indexed field and one non-indexed field returns correct results.
        final String TEST_BUCKET_NAME = "test-intersection-with-two-fields-and-single-index";

        SingleFieldIndexDefinition ageIndex = TestIndexDefinitions.singleField("age-index", "age", BsonType.INT32, false);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size(), "Should return exactly 2 documents with age > 22 and name = John");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldAndQueryWithBatchedIteration() {
        // Behavior: AND query with batch size returns correct batches across multiple cursor advances.
        final String TEST_BUCKET_NAME = "test-intersection-batched-iteration";

        SingleFieldIndexDefinition ageIndex = TestIndexDefinitions.singleField("age-index", "age", BsonType.INT32, false);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().batch(2).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> batch1 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch1.size());
            assertEquals(Set.of(23, 25), extractIntegerFieldFromResults(batch1, "age"));

            List<ByteBuffer> batch2 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch2.size());
            assertEquals(Set.of(27, 29), extractIntegerFieldFromResults(batch2, "age"));

            List<ByteBuffer> batch3 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch3.size());
            assertEquals(Set.of(31, 33), extractIntegerFieldFromResults(batch3, "age"));

            // Batch 4: Should return 1 result (last match)
            List<ByteBuffer> batch4 = readExecutor.execute(tr, ctx);
            assertEquals(1, batch4.size());
            assertEquals(Set.of(35), extractIntegerFieldFromResults(batch4, "age"));

            List<ByteBuffer> batch5 = readExecutor.execute(tr, ctx);
            assertEquals(0, batch5.size());
        }
    }

    @Test
    void shouldAndQueryWithBatchedIterationReverse() {
        // Behavior: AND query with batch size and DESC sort returns correct batches in reverse order.
        final String TEST_BUCKET_NAME = "test-intersection-batched-iteration-reverse";

        SingleFieldIndexDefinition ageIndex = TestIndexDefinitions.singleField("age-index", "age", BsonType.INT32, false);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().batch(2).sortDirection(SortDirection.DESC).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            // Batch 1: Should return 2 results (highest ages first)
            List<ByteBuffer> batch1 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch1.size());
            assertEquals(Set.of(35, 33), extractIntegerFieldFromResults(batch1, "age"));

            List<ByteBuffer> batch2 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch2.size());
            assertEquals(Set.of(31, 29), extractIntegerFieldFromResults(batch2, "age"));

            List<ByteBuffer> batch3 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch3.size());
            assertEquals(Set.of(27, 25), extractIntegerFieldFromResults(batch3, "age"));

            // Batch 4: Should return 1 result (last match)
            List<ByteBuffer> batch4 = readExecutor.execute(tr, ctx);
            assertEquals(1, batch4.size());
            assertEquals(Set.of(23), extractIntegerFieldFromResults(batch4, "age"));

            List<ByteBuffer> batch5 = readExecutor.execute(tr, ctx);
            assertEquals(0, batch5.size());
        }
    }

    @Test
    void shouldAndQueryWithTwoFieldsAndDoubleIndex() {
        // Behavior: AND query with two indexed fields returns correct results using index intersection strategy.
        final String TEST_BUCKET_NAME = "test-intersection-with-two-fields-and-double-index";

        SingleFieldIndexDefinition nameIndex = TestIndexDefinitions.singleField("name-index", "name", BsonType.STRING, false);
        SingleFieldIndexDefinition ageIndex = TestIndexDefinitions.singleField("age-index", "age", BsonType.INT32, false);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size(), "Should return exactly 2 documents with age > 22 and name = John");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldAndQueryWithRangeScanAndDoubleIndex() {
        // Behavior: AND query with range condition and two indexes returns correct results.
        final String TEST_BUCKET_NAME = "test-intersection-with-two-fields-and-double-index";

        SingleFieldIndexDefinition nameIndex = TestIndexDefinitions.singleField("name-index", "name", BsonType.STRING, false);
        SingleFieldIndexDefinition ageIndex = TestIndexDefinitions.singleField("age-index", "age", BsonType.INT32, false);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22, '$lt': 50 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size(), "Should return exactly 2 documents with age > 22 and name = John");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldExecuteAndQueryWithTwoEqPredicates() {
        // Behavior: AND query with two EQ conditions on indexed fields returns the single matching document.

        SingleFieldIndexDefinition ageIndex = TestIndexDefinitions.singleField("age_idx", "age", BsonType.INT32, false);
        SingleFieldIndexDefinition nameIndex = TestIndexDefinitions.singleField("name_idx", "name", BsonType.STRING, false);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex, nameIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 25}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        String query = "{ $and: [ { age: { $eq: 25 } }, { name: { $eq: \"Alice\" } } ] }";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);

        assertNotNull(planWithParams.plan());

        List<String> results = runQueryOnBucket(metadata, query);
        assertEquals(1, results.size());

        String resultJson = results.getFirst();
        assertTrue(resultJson.contains("\"name\": \"Alice\""));
        assertTrue(resultJson.contains("\"age\": 25"));
    }

    @Test
    void shouldHandleAndOperatorWithTwoIndex() {
        // Behavior: The bucket has INT32 indexes on price and quantity and holds four documents. One fails
        // price > 22 and one fails quantity > 80. A query for price > 22 and quantity > 80 returns the
        // other two: (23, 120) and (35, 140).
        final String TEST_BUCKET_NAME = "test-bucket-and-index-scan";

        SingleFieldIndexDefinition priceIndex = TestIndexDefinitions.singleField("price-index", "price", BsonType.INT32, false);
        SingleFieldIndexDefinition quantityIndex = TestIndexDefinitions.singleField("quantity-index", "quantity", BsonType.INT32, false);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 100}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 120}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 80}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 140}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ 'price': { '$gt': 22 }, 'quantity': { '$gt': 80 } }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents matching both conditions");
            assertEquals(Set.of(23, 35), extractIntegerFieldFromResults(results, "price"));
            assertEquals(Set.of(120, 140), extractIntegerFieldFromResults(results, "quantity"));
        }
    }

    @Test
    void shouldHandleEqualityWithMultipleDocuments() {
        // Behavior: The bucket has an INT32 index on age and a STRING index on name. It holds Johns aged
        // 22, 23, 25 and 35, Alice aged 20 and Dennis aged 30. A query for age > 22 and name == 'John'
        // returns the Johns aged 23, 25 and 35.
        final String TEST_BUCKET_NAME = "test-equality-with-multiple-documents";

        SingleFieldIndexDefinition ageIndex = TestIndexDefinitions.singleField("age-index", "age", BsonType.INT32, false);
        SingleFieldIndexDefinition nameIndex = TestIndexDefinitions.singleField("name-index", "name", BsonType.STRING, false);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        // Insert documents where exactly 3 match the AND condition (age > 22 AND name == 'John')
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),    // No match: age <= 22
                BSONUtil.jsonToDocumentThenBytes("{'age': 22, 'name': 'John'}"),   // No match: age <= 22
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'John'}"),   // Match: age > 22 AND name == 'John'
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'John'}"),   // Match: age > 22 AND name == 'John'
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"),   // Match: age > 22 AND name == 'John'
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Dennis'}")      // No match: name != 'John'
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ 'age': {'$gt': 22}, 'name': {'$eq': 'John'} }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22 AND name == 'John'");
            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(23, 25, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }
}
