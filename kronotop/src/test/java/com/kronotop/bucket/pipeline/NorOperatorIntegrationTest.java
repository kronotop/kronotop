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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class NorOperatorIntegrationTest extends BasePipelineTest {

    // Tests for $nor operator (syntactic sugar for $not($or(...)))

    @Test
    void shouldMatchDocumentsFailingAllNorConditions() {
        // Behavior: $nor selects documents that fail ALL the specified query expressions
        final String TEST_BUCKET_NAME = "test-bucket-nor-basic";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'price': 1.99, 'qty': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'price': 2.99, 'qty': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'price': 3.99, 'qty': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'price': 0.99, 'qty': 10}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor: [{price: 1.99}, {qty: {$lt: 20}}]
        // Returns documents where price != 1.99 AND qty >= 20
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'price': 1.99}, {'qty': {'$lt': 20}}]}");

        // $nor uses FullScanNode (since it transforms to $not($or(...)))
        assertInstanceOf(FullScanNode.class, planWithParams.plan(), "Should use FullScanNode for $nor operator");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: price=1.99 OR qty<20? -> 1.99 matches, NOR = false
            // Bob: price=2.99 OR qty=15<20? -> qty matches, NOR = false
            // Charlie: price=3.99 OR qty=30<20? -> neither matches, NOR = true ✓
            // Diana: price=0.99 OR qty=10<20? -> qty matches, NOR = false
            assertEquals(1, results.size(), "Should return 1 document");
            assertEquals(Set.of("Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchAllDocumentsWhenNorConditionsAllFail() {
        // Behavior: $nor returns documents when none of the conditions match any document
        final String TEST_BUCKET_NAME = "test-bucket-nor-all-fail";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'status': 'pending'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor with conditions that no document matches
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'status': 'deleted'}, {'status': 'archived'}]}");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // No document has status='deleted' or 'archived', so all pass $nor
            assertEquals(3, results.size(), "Should return all 3 documents");
            assertEquals(Set.of("Alice", "Bob", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldReturnEmptyWhenAllDocumentsMatchAtLeastOneNorCondition() {
        // Behavior: $nor returns empty when every document matches at least one condition
        final String TEST_BUCKET_NAME = "test-bucket-nor-all-match";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'role': 'admin'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'role': 'user'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'role': 'admin'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor with conditions that cover all documents
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'role': 'admin'}, {'role': 'user'}]}");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // All documents match either admin or user, so $nor returns none
            assertEquals(0, results.size(), "Should return 0 documents");
        }
    }

    @Test
    void shouldHandleNorWithMultipleConditions() {
        // Behavior: $nor with 3+ conditions - must fail ALL to be returned
        final String TEST_BUCKET_NAME = "test-bucket-nor-multi";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active', 'priority': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'status': 'inactive', 'priority': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'status': 'pending', 'priority': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'status': 'review', 'priority': 4}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: NOT (active OR inactive OR pending) -> only review passes
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'status': 'active'}, {'status': 'inactive'}, {'status': 'pending'}]}");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return 1 document");
            assertEquals(Set.of("Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineNorWithAnd() {
        // Behavior: $nor combined with $and in same query
        final String TEST_BUCKET_NAME = "test-bucket-nor-and";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'type': 'food', 'price': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'type': 'drink', 'price': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'type': 'snack', 'price': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'type': 'food', 'price': 8}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor (food OR drink) AND price > 1 -> only snack with price > 1
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'$nor': [{'type': 'food'}, {'type': 'drink'}]}, {'price': {'$gt': 1}}]}");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: food -> NOR fails, excluded
            // Bob: drink -> NOR fails, excluded
            // Charlie: snack, price=2>1 -> NOR passes, AND passes ✓
            // Diana: food -> NOR fails, excluded
            assertEquals(1, results.size(), "Should return 1 document");
            assertEquals(Set.of("Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchAllDocumentsWithEmptyNorArray() {
        // Behavior: {$nor: []} matches all documents because there are no conditions to fail.
        final String TEST_BUCKET_NAME = "test-bucket-nor-empty-array";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'status': 'inactive'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'status': 'pending'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: {$nor: []} - empty array means no conditions to fail
        // Transforms to $not($or([])) where $or([]) = false, so $not(false) = true
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'$nor': []}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // All documents should match since there are no conditions to fail
            assertEquals(3, results.size(), "Empty $nor should match all documents");
            assertEquals(Set.of("Alice", "Bob", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNorWithMissingField() {
        // Behavior: When a document is missing a field, the condition on that field fails,
        // so $nor treats the missing field as NOT matching the condition.
        final String TEST_BUCKET_NAME = "test-bucket-nor-missing-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}"),  // missing status field
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'status': 'inactive'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana'}")  // missing status field
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor [{status: 'active'}, {status: 'inactive'}]
        // Documents with missing 'status' field don't match either condition
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'status': 'active'}, {'status': 'inactive'}]}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: status='active' matches first condition, NOR = false
            // Bob: missing status, doesn't match either condition, NOR = true ✓
            // Charlie: status='inactive' matches second condition, NOR = false
            // Diana: missing status, doesn't match either condition, NOR = true ✓
            assertEquals(2, results.size(), "Should return documents with missing field");
            assertEquals(Set.of("Bob", "Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNorWithNullValues() {
        // Behavior: Null values are treated as actual values, not as missing fields.
        final String TEST_BUCKET_NAME = "test-bucket-nor-null-values";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'status': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'status': 'inactive'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'status': null}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor [{status: 'active'}, {status: null}]
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'status': 'active'}, {'status': null}]}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: status='active' matches first condition, NOR = false
            // Bob: status=null matches second condition, NOR = false
            // Charlie: status='inactive' doesn't match either, NOR = true ✓
            // Diana: status=null matches second condition, NOR = false
            assertEquals(1, results.size(), "Should return documents not matching any condition");
            assertEquals(Set.of("Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNorCombinedWithOr() {
        // Behavior: $nor and $or can be combined in the same query
        final String TEST_BUCKET_NAME = "test-bucket-nor-with-or";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'type': 'admin', 'level': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'type': 'user', 'level': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'type': 'guest', 'level': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'type': 'guest', 'level': 3}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor [admin, user] AND $or [level: 1, level: 3]
        // Should return guests with level 1 or 3
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'$nor': [{'type': 'admin'}, {'type': 'user'}]}, {'$or': [{'level': 1}, {'level': 3}]}]}");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: admin -> NOR fails
            // Bob: user -> NOR fails
            // Charlie: guest (NOR passes), level=1 (OR passes) ✓
            // Diana: guest (NOR passes), level=3 (OR passes) ✓
            assertEquals(2, results.size(), "Should return guests with level 1 or 3");
            assertEquals(Set.of("Charlie", "Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNorWithExists() {
        // Behavior: $nor with $exists checks field presence
        final String TEST_BUCKET_NAME = "test-bucket-nor-exists";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'email': 'alice@test.com', 'phone': '123'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'email': 'bob@test.com'}"),  // no phone
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'phone': '456'}"),  // no email
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana'}")  // no email, no phone
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor [{email: {$exists: true}}, {phone: {$exists: true}}]
        // Returns documents where neither email nor phone exists
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'email': {'$exists': true}}, {'phone': {'$exists': true}}]}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: has email AND phone -> NOR fails
            // Bob: has email -> NOR fails
            // Charlie: has phone -> NOR fails
            // Diana: has neither -> NOR passes ✓
            assertEquals(1, results.size(), "Should return documents without email or phone");
            assertEquals(Set.of("Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNorWithIn() {
        // Behavior: $nor with $in operator
        final String TEST_BUCKET_NAME = "test-bucket-nor-in";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'color': 'red'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'color': 'blue'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'color': 'green'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'color': 'yellow'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor [{color: {$in: ['red', 'blue']}}, {color: {$in: ['green']}}]
        // Returns documents where color is NOT in red/blue AND NOT in green
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'color': {'$in': ['red', 'blue']}}, {'color': {'$in': ['green']}}]}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: red in [red,blue] -> NOR fails
            // Bob: blue in [red,blue] -> NOR fails
            // Charlie: green in [green] -> NOR fails
            // Diana: yellow not in any -> NOR passes ✓
            assertEquals(1, results.size(), "Should return documents with color not in any list");
            assertEquals(Set.of("Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNorWithNin() {
        // Behavior: $nor with $nin operator (double negation)
        final String TEST_BUCKET_NAME = "test-bucket-nor-nin";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'score': 85}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'score': 90}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'score': 75}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'score': 95}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor [{score: {$nin: [85, 90]}}]
        // $nin matches documents NOT in the list, $nor negates that
        // So this returns documents WHERE score IS in [85, 90]
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'score': {'$nin': [85, 90]}}]}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: 85 in list, $nin = false, NOR(false) = true ✓
            // Bob: 90 in list, $nin = false, NOR(false) = true ✓
            // Charlie: 75 not in list, $nin = true, NOR(true) = false
            // Diana: 95 not in list, $nin = true, NOR(true) = false
            assertEquals(2, results.size(), "Should return documents with score in list");
            assertEquals(Set.of("Alice", "Bob"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNestedNor() {
        // Behavior: $nor can be nested inside $nor (unusual but valid)
        final String TEST_BUCKET_NAME = "test-bucket-nor-nested";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'x': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'x': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'x': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'x': 4}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: $nor [$nor [{x: 1}, {x: 2}]]
        // Inner $nor: NOT(x=1 OR x=2) = x NOT IN [1,2] -> matches Charlie, Diana
        // Outer $nor: NOT(inner) -> matches Alice, Bob
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$nor': [{'$nor': [{'x': 1}, {'x': 2}]}]}");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Inner $nor matches Charlie (x=3) and Diana (x=4)
            // Outer $nor negates, so returns Alice (x=1) and Bob (x=2)
            assertEquals(2, results.size(), "Should return documents matching double negation");
            assertEquals(Set.of("Alice", "Bob"), extractNamesFromResults(results));
        }
    }
}
