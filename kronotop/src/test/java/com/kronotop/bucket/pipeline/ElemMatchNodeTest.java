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
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ElemMatchNodeTest extends BasePipelineTest {

    @Test
    void shouldMatchDocumentsWithElemMatchOnArrayOfDocuments() {
        // Behavior: $elemMatch on an array of documents matches documents where at least one
        // array element satisfies the specified condition (e.g., price > 100).

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-basic";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert documents with array of items
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'price': 50, 'category': 'toys'}, {'price': 80, 'category': 'books'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'price': 150, 'category': 'electronics'}, {'price': 200, 'category': 'electronics'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'price': 30, 'category': 'toys'}, {'price': 40, 'category': 'toys'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders where at least one item has price > 100
        String query = "{'items': {'$elemMatch': {'price': {'$gt': 100}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Order2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithMultipleConditionsInElemMatch() {
        // Behavior: $elemMatch with multiple conditions requires a SINGLE array element to satisfy
        // ALL conditions simultaneously. Different elements satisfying different conditions won't match.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-multi-cond";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert documents with array of items
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'price': 150, 'category': 'toys'}, {'price': 80, 'category': 'electronics'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'price': 150, 'category': 'electronics'}, {'price': 200, 'category': 'books'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'price': 30, 'category': 'electronics'}, {'price': 40, 'category': 'toys'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item that is electronics AND price > 100
        String query = "{'items': {'$elemMatch': {'price': {'$gt': 100}, 'category': 'electronics'}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Order2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldReturnEmptyWhenNoElementMatchesAllConditions() {
        // Behavior: When no single array element satisfies all $elemMatch conditions, the document
        // is not matched even if different elements satisfy individual conditions separately.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-no-match";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert documents where no single element satisfies all conditions
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'price': 50, 'category': 'electronics'}, {'price': 150, 'category': 'toys'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item that is electronics AND price > 100
        // Neither element satisfies both conditions
        String query = "{'items': {'$elemMatch': {'price': {'$gt': 100}, 'category': 'electronics'}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertTrue(results.isEmpty(), "Should return no documents");
        }
    }

    @Test
    void shouldHandleEmptyArrayField() {
        // Behavior: Documents with empty arrays never match $elemMatch queries since there are
        // no elements to evaluate against the conditions.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-empty-array";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': []}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'price': 150}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{'items': {'$elemMatch': {'price': {'$gt': 100}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Order2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleMissingArrayField() {
        // Behavior: Documents missing the array field specified in $elemMatch do not match,
        // as there is no array to evaluate.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-missing-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'otherField': 'value'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'price': 150}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{'items': {'$elemMatch': {'price': {'$gt': 100}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Order2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithNestedDocumentFields() {
        // Behavior: $elemMatch supports dot notation to access nested fields within array
        // elements (e.g., 'details.amount' accesses amount inside nested details object).

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-nested";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'orders': [{'details': {'amount': 100}}, {'details': {'amount': 50}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'orders': [{'details': {'amount': 200}}, {'details': {'amount': 150}}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{'orders': {'$elemMatch': {'details.amount': {'$gte': 150}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Order2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithEqOperatorInElemMatch() {
        // Behavior: Implicit equality in $elemMatch (without explicit $eq) matches array
        // elements where the specified field equals the given value exactly.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-eq";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'status': 'pending'}, {'status': 'shipped'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'status': 'delivered'}, {'status': 'shipped'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'status': 'pending'}, {'status': 'pending'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{'items': {'$elemMatch': {'status': 'delivered'}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Order2"), extractNamesFromResults(results));
        }
    }

    // Scalar Array Tests

    @Test
    void shouldMatchScalarStringArrayWithEq() {
        // Behavior: $elemMatch with $eq on scalar string arrays matches documents where at least
        // one string element exactly equals the specified value.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-string-eq";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'tags': ['bug', 'feature', 'enhancement']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'tags': ['urgent', 'bug', 'critical']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'tags': ['documentation', 'help-wanted']}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one 'urgent' tag
        String query = "{'tags': {'$elemMatch': {'$eq': 'urgent'}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Task2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchScalarNumberArrayWithGt() {
        // Behavior: $elemMatch with $gt on scalar number arrays matches documents where at least
        // one numeric element exceeds the specified threshold.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-number-gt";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student1', 'scores': [75, 82, 88]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student2', 'scores': [91, 95, 89]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student3', 'scores': [65, 70, 72]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find students with at least one score > 90
        String query = "{'scores': {'$elemMatch': {'$gt': 90}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Student2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchScalarNumberArrayWithLte() {
        // Behavior: $elemMatch with $lte on scalar number arrays matches documents where at least
        // one numeric element is less than or equal to the specified value.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-number-lte";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'prices': [29.99, 39.99, 49.99]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'prices': [9.99, 14.99, 19.99]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'prices': [99.99, 149.99, 199.99]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find products with at least one price <= 10.0
        String query = "{'prices': {'$elemMatch': {'$lte': 10.0}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Product2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchScalarBooleanArray() {
        // Behavior: $elemMatch with $eq on scalar boolean arrays matches documents where at least
        // one boolean element equals the specified true/false value.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-boolean";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config1', 'flags': [false, false, false]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config2', 'flags': [true, false, true]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config3', 'flags': [false, false, false]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find configs with at least one true flag
        String query = "{'flags': {'$elemMatch': {'$eq': true}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Config2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldNotMatchScalarArrayWhenNoElementSatisfiesCondition() {
        // Behavior: When no element in a scalar array satisfies the $elemMatch condition,
        // the document is excluded from results.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-no-match";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item1', 'values': [10, 20, 30]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item2', 'values': [40, 50, 60]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find items with at least one value > 100 (none exist)
        String query = "{'values': {'$elemMatch': {'$gt': 100}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertTrue(results.isEmpty(), "Should return no documents");
        }
    }

    @Test
    void shouldHandleEmptyScalarArray() {
        // Behavior: Empty scalar arrays never match $elemMatch queries since there are no
        // elements to evaluate against the conditions.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-empty";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Empty', 'tags': []}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'NonEmpty', 'tags': ['urgent']}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{'tags': {'$elemMatch': {'$eq': 'urgent'}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("NonEmpty"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchScalarStringArrayWithNe() {
        // Behavior: $elemMatch with $ne on scalar string arrays matches documents where at least
        // one element does NOT equal the specified value.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-string-ne";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'statuses': ['pending', 'pending', 'pending']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'statuses': ['completed', 'pending', 'completed']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'statuses': ['pending', 'pending']}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one status != 'pending'
        String query = "{'statuses': {'$elemMatch': {'$ne': 'pending'}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Task2"), extractNamesFromResults(results));
        }
    }

    // $in operator inside $elemMatch

    @Test
    void shouldMatchWithInOperatorInsideElemMatch() {
        // Behavior: $in inside $elemMatch matches documents where at least one array element
        // has a field value that exists in the specified list of values.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-in-inside";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'status': 'pending'}, {'status': 'processing'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'status': 'shipped'}, {'status': 'delivered'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'status': 'cancelled'}, {'status': 'refunded'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item with status in ['shipped', 'delivered']
        String query = "{'items': {'$elemMatch': {'status': {'$in': ['shipped', 'delivered']}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Order2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchScalarArrayWithInOperatorInsideElemMatch() {
        // Behavior: $in inside $elemMatch on scalar arrays matches documents where at least
        // one array element exists in the specified list of values.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-in";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'tags': ['bug', 'feature']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'tags': ['urgent', 'critical']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'tags': ['documentation', 'help-wanted']}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one tag in ['urgent', 'critical', 'blocker']
        String query = "{'tags': {'$elemMatch': {'$in': ['urgent', 'critical', 'blocker']}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Task2"), extractNamesFromResults(results));
        }
    }

    // $nin operator inside $elemMatch

    @Test
    void shouldMatchWithNinOperatorInsideElemMatch() {
        // Behavior: $nin inside $elemMatch matches documents where at least one array element
        // has a field value that does NOT exist in the specified exclusion list.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-nin-inside";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'status': 'pending'}, {'status': 'shipped'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'status': 'cancelled'}, {'status': 'refunded'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'status': 'pending'}, {'status': 'processing'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item with status NOT in ['cancelled', 'refunded', 'failed']
        String query = "{'items': {'$elemMatch': {'status': {'$nin': ['cancelled', 'refunded', 'failed']}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order1", "Order3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchScalarArrayWithNinOperatorInsideElemMatch() {
        // Behavior: $nin inside $elemMatch on scalar arrays matches documents where at least
        // one array element is NOT in the specified exclusion list.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-nin";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'priorities': [1, 2, 3]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'priorities': [4, 5]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'priorities': [1, 1, 1]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one priority NOT in [1, 2, 3]
        String query = "{'priorities': {'$elemMatch': {'$nin': [1, 2, 3]}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Task2"), extractNamesFromResults(results));
        }
    }

    // Null values in arrays

    @Test
    void shouldHandleNullValuesInArrayOfDocuments() {
        // Behavior: Null field values within array elements are properly handled. Comparison
        // operators like $gt skip null values; only non-null values are evaluated.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-null-docs";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc1', 'items': [{'value': 10}, {'value': null}, {'value': 30}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc2', 'items': [{'value': null}, {'value': null}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc3', 'items': [{'value': 50}, {'value': 60}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find documents with at least one item where value > 20
        String query = "{'items': {'$elemMatch': {'value': {'$gt': 20}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Doc1", "Doc3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNullValuesInScalarArray() {
        // Behavior: Null values in scalar arrays are properly handled. Comparison operators
        // skip null elements; only non-null elements are evaluated against conditions.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-null-scalar";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item1', 'values': [10, null, 30]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item2', 'values': [null, null]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item3', 'values': [50, 60]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find items with at least one value > 25
        String query = "{'values': {'$elemMatch': {'$gt': 25}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Item1", "Item3"), extractNamesFromResults(results));
        }
    }

    // Single-element arrays

    @Test
    void shouldMatchSingleElementArrayOfDocuments() {
        // Behavior: $elemMatch works correctly with single-element arrays of documents,
        // matching if the sole element satisfies all specified conditions.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-single-doc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'price': 150}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'price': 50}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'price': 200}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item with price > 100
        String query = "{'items': {'$elemMatch': {'price': {'$gt': 100}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order1", "Order3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchSingleElementScalarArray() {
        // Behavior: $elemMatch works correctly with single-element scalar arrays, matching
        // if the sole element satisfies the specified condition.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-single-scalar";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'scores': [95]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'scores': [70]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'scores': [85]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one score >= 80
        String query = "{'scores': {'$elemMatch': {'$gte': 80}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Task1", "Task3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldNotMatchSingleElementArrayWhenConditionNotSatisfied() {
        // Behavior: Single-element arrays that don't satisfy the $elemMatch condition result
        // in the document being excluded from results.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-single-no-match";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item1', 'values': [50]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item2', 'values': [60]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find items with at least one value > 100 (none exist)
        String query = "{'values': {'$elemMatch': {'$gt': 100}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertTrue(results.isEmpty(), "Should return no documents");
        }
    }

    // $exists operator inside $elemMatch

    @Test
    void shouldMatchWithExistsInsideElemMatch() {
        // Behavior: $exists: true inside $elemMatch matches documents where at least one array
        // element contains the specified field, regardless of its value.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-exists";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'variants': [{'color': 'red', 'size': 'M'}, {'color': 'blue'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'variants': [{'color': 'green'}, {'color': 'yellow'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'variants': [{'color': 'black', 'size': 'L'}, {'color': 'white', 'size': 'S'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find products with at least one variant that has a 'size' field
        String query = "{'variants': {'$elemMatch': {'size': {'$exists': true}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product1", "Product3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithExistsFalseInsideElemMatch() {
        // Behavior: $exists: false inside $elemMatch matches documents where at least one
        // array element is MISSING the specified field.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-exists-false";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'variants': [{'color': 'red', 'size': 'M'}, {'color': 'blue'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'variants': [{'color': 'green', 'size': 'L'}, {'color': 'yellow', 'size': 'S'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'variants': [{'color': 'black'}, {'color': 'white'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find products with at least one variant that does NOT have a 'size' field
        String query = "{'variants': {'$elemMatch': {'size': {'$exists': false}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product1", "Product3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineExistsWithOtherConditionsInsideElemMatch() {
        // Behavior: $exists can be combined with other operators on the same field. The element
        // must satisfy ALL conditions (e.g., field exists AND value > threshold).

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-exists-combined";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'variants': [{'color': 'red', 'price': 100}, {'color': 'blue', 'price': 50}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'variants': [{'color': 'green'}, {'color': 'yellow', 'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'variants': [{'color': 'black', 'price': 150}, {'color': 'white', 'price': 80}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find products with at least one variant that has a 'price' field AND price > 120
        String query = "{'variants': {'$elemMatch': {'price': {'$exists': true, '$gt': 120}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product2", "Product3"), extractNamesFromResults(results));
        }
    }

    // Explicit $or inside $elemMatch

    @Test
    void shouldMatchWithOrInsideElemMatchOnDocumentArray() {
        // Behavior: $or inside $elemMatch allows matching elements that satisfy ANY of the
        // specified conditions, combined with other conditions using implicit AND.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-or-doc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student1', 'scores': [{'subject': 'math', 'score': 85}, {'subject': 'history', 'score': 75}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student2', 'scores': [{'subject': 'science', 'score': 90}, {'subject': 'art', 'score': 60}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student3', 'scores': [{'subject': 'english', 'score': 88}, {'subject': 'music', 'score': 70}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student4', 'scores': [{'subject': 'math', 'score': 92}, {'subject': 'science', 'score': 85}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find students with at least one score where (subject is 'math' OR subject is 'science') AND score > 80
        String query = "{'scores': {'$elemMatch': {'$or': [{'subject': 'math'}, {'subject': 'science'}], 'score': {'$gt': 80}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Student1: math=85 > 80 ✓
            // Student2: science=90 > 80 ✓
            // Student3: no math/science
            // Student4: math=92 > 80 ✓, science=85 > 80 ✓
            assertEquals(3, results.size(), "Should return exactly 3 documents");
            assertEquals(Set.of("Student1", "Student2", "Student4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithOrInsideElemMatchOnScalarArray() {
        // Behavior: $or inside $elemMatch on scalar arrays matches elements where the value
        // satisfies ANY of the specified conditions.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-or-scalar";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'tags': ['bug', 'critical']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'tags': ['feature', 'enhancement']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'tags': ['urgent', 'bug']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task4', 'tags': ['documentation', 'help-wanted']}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one tag that is 'critical' OR 'urgent'
        String query = "{'tags': {'$elemMatch': {'$or': [{'$eq': 'critical'}, {'$eq': 'urgent'}]}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Task1", "Task3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithNestedOrAndInsideElemMatch() {
        // Behavior: Nested $or and $and operators inside $elemMatch support complex boolean
        // logic where the element must match the entire logical expression.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-nested-or-and";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'type': 'electronics', 'price': 500, 'inStock': true}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'type': 'books', 'price': 30, 'inStock': true}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'type': 'electronics', 'price': 100, 'inStock': false}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'type': 'furniture', 'price': 800, 'inStock': true}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item that is:
        // (type='electronics' AND price > 200) OR (type='furniture' AND inStock=true)
        String query = "{'items': {'$elemMatch': {'$or': [{'$and': [{'type': 'electronics'}, {'price': {'$gt': 200}}]}, {'$and': [{'type': 'furniture'}, {'inStock': true}]}]}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: electronics, price=500 > 200 ✓
            // Order2: books - no match
            // Order3: electronics, price=100 < 200 - no match
            // Order4: furniture, inStock=true ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order1", "Order4"), extractNamesFromResults(results));
        }
    }

    // Explicit $and inside $elemMatch

    @Test
    void shouldMatchWithAndInsideElemMatchOnDocumentArray() {
        // Behavior: Explicit $and inside $elemMatch requires the same array element to satisfy
        // ALL specified conditions simultaneously.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-and-doc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'reviews': [{'rating': 5, 'verified': true}, {'rating': 3, 'verified': false}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'reviews': [{'rating': 4, 'verified': true}, {'rating': 4, 'verified': true}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'reviews': [{'rating': 5, 'verified': false}, {'rating': 2, 'verified': true}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'reviews': [{'rating': 5, 'verified': true}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find products with at least one review that has rating >= 5 AND verified = true
        String query = "{'reviews': {'$elemMatch': {'$and': [{'rating': {'$gte': 5}}, {'verified': true}]}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Product1: rating=5, verified=true ✓
            // Product2: rating=4 < 5 - no match
            // Product3: rating=5 but verified=false, or rating=2 - no match
            // Product4: rating=5, verified=true ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product1", "Product4"), extractNamesFromResults(results));
        }
    }

    // $all inside $elemMatch

    @Test
    void shouldMatchWithAllInsideElemMatchOnDocumentArray() {
        // Behavior: $all inside $elemMatch requires that an array field within the matched
        // element contains ALL specified values.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-all-doc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'variants': [{'tags': ['red', 'large', 'sale']}, {'tags': ['blue', 'medium']}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'variants': [{'tags': ['green', 'small']}, {'tags': ['yellow', 'large']}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'variants': [{'tags': ['red', 'large']}, {'tags': ['red', 'sale']}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'variants': [{'tags': ['red', 'large', 'sale', 'clearance']}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find products with at least one variant that has ALL of ['red', 'large', 'sale'] tags
        String query = "{'variants': {'$elemMatch': {'tags': {'$all': ['red', 'large', 'sale']}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Product1: first variant has all three ✓
            // Product2: no variant has all three
            // Product3: no single variant has all three
            // Product4: has all three ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product1", "Product4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithAllInsideElemMatchCombinedWithOtherConditions() {
        // Behavior: $all can be combined with other conditions inside $elemMatch. The element
        // must have an array containing all specified values AND satisfy other conditions.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-all-combined";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item1', 'items': [{'categories': ['electronics', 'sale'], 'price': 100}, {'categories': ['books'], 'price': 20}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item2', 'items': [{'categories': ['electronics', 'sale'], 'price': 500}, {'categories': ['electronics'], 'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item3', 'items': [{'categories': ['electronics'], 'price': 150}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item4', 'items': [{'categories': ['electronics', 'sale', 'clearance'], 'price': 75}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find items with at least one item that has ALL of ['electronics', 'sale'] AND price < 200
        String query = "{'items': {'$elemMatch': {'categories': {'$all': ['electronics', 'sale']}, 'price': {'$lt': 200}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Item1: first item has both categories AND price=100 < 200 ✓
            // Item2: first item has both categories BUT price=500 >= 200
            // Item3: no item has both categories
            // Item4: has both categories AND price=75 < 200 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Item1", "Item4"), extractNamesFromResults(results));
        }
    }

    // $size inside $elemMatch

    @Test
    void shouldMatchWithSizeInsideElemMatchOnDocumentArray() {
        // Behavior: $size inside $elemMatch matches elements where an array field has exactly
        // the specified number of elements.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-size-doc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'tags': ['a', 'b', 'c']}, {'tags': ['x', 'y']}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'tags': ['p', 'q']}, {'tags': ['r', 's']}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'tags': ['one', 'two', 'three']}, {'tags': ['four', 'five', 'six']}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'tags': []}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item that has exactly 3 tags
        String query = "{'items': {'$elemMatch': {'tags': {'$size': 3}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: first item has 3 tags ✓
            // Order2: no item has 3 tags
            // Order3: both items have 3 tags ✓
            // Order4: item has 0 tags
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order1", "Order3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithSizeZeroInsideElemMatch() {
        // Behavior: $size: 0 inside $elemMatch matches elements that contain an empty array
        // for the specified field.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-size-zero";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc1', 'entries': [{'values': []}, {'values': [1, 2]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc2', 'entries': [{'values': [1]}, {'values': [2, 3]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc3', 'entries': [{'values': []}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc4', 'entries': [{'values': [1, 2, 3]}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find documents with at least one entry that has empty values array
        String query = "{'entries': {'$elemMatch': {'values': {'$size': 0}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Doc1: first entry has empty values ✓
            // Doc2: no entry has empty values
            // Doc3: entry has empty values ✓
            // Doc4: no entry has empty values
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Doc1", "Doc3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithSizeCombinedWithOtherConditionsInsideElemMatch() {
        // Behavior: $size can be combined with other conditions inside $elemMatch. The element
        // must have an array of the exact size AND satisfy all other conditions.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-size-combined";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'assignees': [{'users': ['alice', 'bob'], 'priority': 'high'}, {'users': ['charlie'], 'priority': 'low'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'assignees': [{'users': ['dave', 'eve'], 'priority': 'low'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'assignees': [{'users': ['frank', 'grace'], 'priority': 'high'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task4', 'assignees': [{'users': ['helen'], 'priority': 'high'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one assignee group that has exactly 2 users AND high priority
        String query = "{'assignees': {'$elemMatch': {'users': {'$size': 2}, 'priority': 'high'}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Task1: first assignee has 2 users AND high priority ✓
            // Task2: has 2 users but low priority
            // Task3: has 2 users AND high priority ✓
            // Task4: has 1 user (not 2)
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Task1", "Task3"), extractNamesFromResults(results));
        }
    }

    // Operator combinations inside $elemMatch

    @Test
    void shouldMatchWithAllAndSizeCombinedInsideElemMatch() {
        // Behavior: Combining $all and $size inside $elemMatch requires the array field to
        // contain ALL specified values AND have exactly the specified length.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-all-size";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                // tags has 'red' and 'large' AND exactly 3 elements
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'items': [{'tags': ['red', 'large', 'sale']}, {'tags': ['blue']}]}"),
                // tags has 'red' and 'large' but 4 elements (not 3)
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'items': [{'tags': ['red', 'large', 'sale', 'clearance']}]}"),
                // tags has 'red' and 'large' AND exactly 3 elements
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'items': [{'tags': ['red', 'large', 'premium']}]}"),
                // tags has only 'red' (missing 'large')
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'items': [{'tags': ['red', 'small', 'sale']}]}"),
                // tags has 'red' and 'large' but only 2 elements
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product5', 'items': [{'tags': ['red', 'large']}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find products with at least one item where tags contains ALL of ['red', 'large'] AND has exactly 3 elements
        String query = "{'items': {'$elemMatch': {'tags': {'$all': ['red', 'large'], '$size': 3}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);

        // $all and $size operators cannot use indexes, so FullScanNode is expected
        assertInstanceOf(FullScanNode.class, planWithParams.plan(), "Should use FullScanNode for $all + $size combination");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Product1: first item has ['red', 'large', 'sale'] - has both AND size=3 ✓
            // Product2: has ['red', 'large', 'sale', 'clearance'] - has both BUT size=4
            // Product3: has ['red', 'large', 'premium'] - has both AND size=3 ✓
            // Product4: has ['red', 'small', 'sale'] - missing 'large'
            // Product5: has ['red', 'large'] - has both BUT size=2
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product1", "Product3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithNinAndExistsCombinedInsideElemMatch() {
        // Behavior: Combining $nin and $exists inside $elemMatch matches elements where the
        // field exists AND its value is NOT in the exclusion list.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-nin-exists";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                // status exists and is 'active' (not in excluded list)
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'status': 'active', 'qty': 5}, {'status': 'cancelled', 'qty': 2}]}"),
                // status exists but is 'cancelled' (in excluded list)
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'status': 'cancelled', 'qty': 3}, {'status': 'deleted', 'qty': 1}]}"),
                // status does not exist on any item
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'qty': 10}, {'qty': 20}]}"),
                // status exists and is 'shipped' (not in excluded list)
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'status': 'shipped', 'qty': 7}]}"),
                // mixed: one item has no status, one has 'pending' (not excluded)
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order5', 'items': [{'qty': 1}, {'status': 'pending', 'qty': 2}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item where status exists AND is NOT in ['cancelled', 'deleted']
        String query = "{'items': {'$elemMatch': {'status': {'$exists': true, '$nin': ['cancelled', 'deleted']}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);

        // $nin and $exists operators cannot use indexes, so FullScanNode is expected
        assertInstanceOf(FullScanNode.class, planWithParams.plan(), "Should use FullScanNode for $nin + $exists combination");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: first item has status='active' which exists and not in excluded list ✓
            // Order2: all items have status in excluded list ['cancelled', 'deleted']
            // Order3: no items have status field
            // Order4: has status='shipped' which exists and not in excluded list ✓
            // Order5: second item has status='pending' which exists and not in excluded list ✓
            assertEquals(3, results.size(), "Should return exactly 3 documents");
            assertEquals(Set.of("Order1", "Order4", "Order5"), extractNamesFromResults(results));
        }
    }

    // Nested $elemMatch
    @Test
    void shouldMatchWithNestedElemMatch() {
        // Behavior: Nested $elemMatch allows querying arrays within arrays. The outer $elemMatch
        // finds elements, and the inner $elemMatch evaluates nested arrays within those elements.

        final String TEST_BUCKET_NAME = "test-bucket-nested-elemmatch";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store1', 'departments': [{'name': 'Electronics', 'products': [{'sku': 'TV1', 'price': 500}, {'sku': 'TV2', 'price': 800}]}, {'name': 'Books', 'products': [{'sku': 'B1', 'price': 20}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store2', 'departments': [{'name': 'Electronics', 'products': [{'sku': 'PC1', 'price': 1200}]}, {'name': 'Clothing', 'products': [{'sku': 'S1', 'price': 50}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store3', 'departments': [{'name': 'Food', 'products': [{'sku': 'F1', 'price': 10}, {'sku': 'F2', 'price': 15}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store4', 'departments': [{'name': 'Electronics', 'products': [{'sku': 'CAM1', 'price': 300}, {'sku': 'CAM2', 'price': 450}]}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find stores with at least one department that has at least one product with price > 400
        String query = "{'departments': {'$elemMatch': {'products': {'$elemMatch': {'price': {'$gt': 400}}}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Store1: Electronics has TV1=500 and TV2=800 > 400 ✓
            // Store2: Electronics has PC1=1200 > 400 ✓
            // Store3: Food has no product > 400
            // Store4: Electronics has CAM2=450 > 400 ✓
            assertEquals(3, results.size(), "Should return exactly 3 documents");
            assertEquals(Set.of("Store1", "Store2", "Store4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithNestedElemMatchAndMultipleConditions() {
        // Behavior: Nested $elemMatch with multiple conditions requires a nested element to
        // satisfy ALL conditions (e.g., role='engineer' AND level >= 4).

        final String TEST_BUCKET_NAME = "test-bucket-nested-elemmatch-multi";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company1', 'teams': [{'name': 'Dev', 'members': [{'role': 'engineer', 'level': 3}, {'role': 'engineer', 'level': 5}]}, {'name': 'QA', 'members': [{'role': 'tester', 'level': 2}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company2', 'teams': [{'name': 'Dev', 'members': [{'role': 'engineer', 'level': 2}, {'role': 'manager', 'level': 4}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company3', 'teams': [{'name': 'Sales', 'members': [{'role': 'sales', 'level': 3}]}, {'name': 'Dev', 'members': [{'role': 'engineer', 'level': 4}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company4', 'teams': [{'name': 'Support', 'members': [{'role': 'support', 'level': 2}]}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find companies with at least one team that has at least one member who is an engineer with level >= 4
        String query = "{'teams': {'$elemMatch': {'members': {'$elemMatch': {'role': 'engineer', 'level': {'$gte': 4}}}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Company1: Dev team has engineer level 5 >= 4 ✓
            // Company2: Dev team has engineer level 2 < 4
            // Company3: Dev team has engineer level 4 >= 4 ✓
            // Company4: no engineers
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Company1", "Company3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithTripleNestedElemMatch() {
        // Behavior: Triple-nested $elemMatch traverses three levels of arrays, finding documents
        // where each nesting level has at least one element satisfying the conditions.

        final String TEST_BUCKET_NAME = "test-bucket-triple-nested-elemmatch";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Org1', 'divisions': [{'units': [{'groups': [{'score': 95}, {'score': 80}]}, {'groups': [{'score': 70}]}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Org2', 'divisions': [{'units': [{'groups': [{'score': 60}, {'score': 65}]}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Org3', 'divisions': [{'units': [{'groups': [{'score': 88}]}]}, {'units': [{'groups': [{'score': 92}]}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Org4', 'divisions': [{'units': [{'groups': [{'score': 50}]}]}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orgs with a division that has a unit that has a group with score > 90
        String query = "{'divisions': {'$elemMatch': {'units': {'$elemMatch': {'groups': {'$elemMatch': {'score': {'$gt': 90}}}}}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Org1: has group with score 95 > 90 ✓
            // Org2: max score is 65 < 90
            // Org3: has group with score 92 > 90 ✓
            // Org4: score 50 < 90
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Org1", "Org3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithMultipleElemMatchOnSameField() {
        // Behavior: Multiple $elemMatch on the same field allows conditions to be satisfied by
        // DIFFERENT elements (unlike single $elemMatch where same element must satisfy all).

        final String TEST_BUCKET_NAME = "test-bucket-multi-elemmatch-same-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'price': 50, 'qty': 2}, {'price': 150, 'qty': 5}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'price': 200, 'qty': 1}, {'price': 30, 'qty': 10}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'price': 100, 'qty': 3}, {'price': 80, 'qty': 4}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'price': 120, 'qty': 6}, {'price': 90, 'qty': 8}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: items has element with price > 100 AND items has element with qty > 5
        // These can be DIFFERENT elements (unlike single $elemMatch which requires same element)
        String query = "{'items': {'$elemMatch': {'price': {'$gt': 100}}}, 'items': {'$elemMatch': {'qty': {'$gt': 5}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: price 150 > 100 ✓, qty 5 not > 5
            // Order2: price 200 > 100 ✓, qty 10 > 5 ✓
            // Order3: price 100 not > 100
            // Order4: price 120 > 100 ✓, qty 6 and 8 > 5 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order2", "Order4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithDeeplyNestedFieldInsideElemMatch() {
        // Behavior: Deeply nested dot notation paths (e.g., 'details.info.value') inside $elemMatch
        // navigate through multiple levels of embedded documents within array elements.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-deep-nested";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record1', 'items': [{'details': {'info': {'value': 100}}}, {'details': {'info': {'value': 50}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record2', 'items': [{'details': {'info': {'value': 200}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record3', 'items': [{'details': {'info': {'value': 30}}}, {'details': {'info': {'value': 40}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record4', 'items': [{'details': {'info': {'value': 150}}}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find records with at least one item where details.info.value > 120
        String query = "{'items': {'$elemMatch': {'details.info.value': {'$gt': 120}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Record1: max value 100 < 120
            // Record2: value 200 > 120 ✓
            // Record3: max value 40 < 120
            // Record4: value 150 > 120 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Record2", "Record4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithMultipleDeeplyNestedFieldsInsideElemMatch() {
        // Behavior: Multiple deeply nested field conditions inside $elemMatch require the same
        // array element to satisfy all nested path conditions simultaneously.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-multi-deep";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data1', 'entries': [{'meta': {'stats': {'count': 50, 'total': 500}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data2', 'entries': [{'meta': {'stats': {'count': 100, 'total': 800}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data3', 'entries': [{'meta': {'stats': {'count': 80, 'total': 1200}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data4', 'entries': [{'meta': {'stats': {'count': 120, 'total': 1500}}}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find data with entry where meta.stats.count > 70 AND meta.stats.total > 1000
        String query = "{'entries': {'$elemMatch': {'meta.stats.count': {'$gt': 70}, 'meta.stats.total': {'$gt': 1000}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Data1: count 50 < 70
            // Data2: count 100 > 70 ✓, total 800 < 1000
            // Data3: count 80 > 70 ✓, total 1200 > 1000 ✓
            // Data4: count 120 > 70 ✓, total 1500 > 1000 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Data3", "Data4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithCompoundRangeInsideElemMatchOnDocumentArray() {
        // Behavior: Compound range conditions ($gte + $lte) on the same field inside $elemMatch
        // require the value to fall within the specified range, combined with other conditions.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-compound-range";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'variants': [{'price': 50, 'stock': 100}, {'price': 150, 'stock': 20}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'variants': [{'price': 80, 'stock': 50}, {'price': 120, 'stock': 30}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'variants': [{'price': 200, 'stock': 10}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'variants': [{'price': 90, 'stock': 75}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find products with variant where price is between 60 and 130 AND stock > 40
        String query = "{'variants': {'$elemMatch': {'price': {'$gte': 60, '$lte': 130}, 'stock': {'$gt': 40}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Product1: price 50 not in [60,130] for first, price 150 not in range for second
            // Product2: price 80 in [60,130] ✓, stock 50 > 40 ✓
            // Product3: price 200 not in [60,130]
            // Product4: price 90 in [60,130] ✓, stock 75 > 40 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product2", "Product4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithCompoundRangeInsideElemMatchOnScalarArray() {
        // Behavior: Compound range conditions on scalar arrays match elements where the value
        // falls within the specified range (e.g., >= 30 AND <= 50).

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-compound-range";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Sensor1', 'readings': [15, 25, 35]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Sensor2', 'readings': [45, 55, 65]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Sensor3', 'readings': [20, 40, 60]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Sensor4', 'readings': [5, 10, 15]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find sensors with at least one reading between 30 and 50 (inclusive)
        String query = "{'readings': {'$elemMatch': {'$gte': 30, '$lte': 50}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Sensor1: 35 in [30,50] ✓
            // Sensor2: 45 in [30,50] ✓
            // Sensor3: 40 in [30,50] ✓
            // Sensor4: max 15 < 30
            assertEquals(3, results.size(), "Should return exactly 3 documents");
            assertEquals(Set.of("Sensor1", "Sensor2", "Sensor3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithNeInsideElemMatchOnDocumentArray() {
        // Behavior: $ne inside $elemMatch on document arrays matches documents where at least
        // one element has a field value that is NOT equal to the specified value.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-ne-doc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'status': 'pending'}, {'status': 'shipped'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'status': 'cancelled'}, {'status': 'cancelled'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'status': 'delivered'}, {'status': 'cancelled'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'status': 'cancelled'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item with status != 'cancelled'
        String query = "{'items': {'$elemMatch': {'status': {'$ne': 'cancelled'}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: pending and shipped != cancelled ✓
            // Order2: all cancelled
            // Order3: delivered != cancelled ✓
            // Order4: only cancelled
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order1", "Order3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithNeInsideElemMatchOnScalarArray() {
        // Behavior: $ne inside $elemMatch on scalar arrays matches documents where at least
        // one element is NOT equal to the specified value.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-ne-scalar";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config1', 'values': [0, 0, 0]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config2', 'values': [0, 1, 0]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config3', 'values': [1, 2, 3]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config4', 'values': [0, 0]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find configs with at least one value != 0
        String query = "{'values': {'$elemMatch': {'$ne': 0}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Config1: all zeros
            // Config2: 1 != 0 ✓
            // Config3: all != 0 ✓
            // Config4: all zeros
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Config2", "Config3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineNeWithOtherConditionsInsideElemMatch() {
        // Behavior: $ne can be combined with equality conditions inside $elemMatch. The element
        // must satisfy ALL conditions (e.g., user='alice' AND role != 'viewer').

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-ne-combined";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'assignments': [{'user': 'alice', 'role': 'owner'}, {'user': 'bob', 'role': 'viewer'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'assignments': [{'user': 'charlie', 'role': 'editor'}, {'user': 'dave', 'role': 'owner'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'assignments': [{'user': 'eve', 'role': 'viewer'}, {'user': 'frank', 'role': 'viewer'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task4', 'assignments': [{'user': 'alice', 'role': 'editor'}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one assignment where user = 'alice' AND role != 'viewer'
        String query = "{'assignments': {'$elemMatch': {'user': 'alice', 'role': {'$ne': 'viewer'}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Task1: alice with owner role != viewer ✓
            // Task2: no alice
            // Task3: no alice
            // Task4: alice with editor role != viewer ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Task1", "Task4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithLargeArrays() {
        // Behavior: $elemMatch correctly handles large arrays (50+ elements), efficiently
        // scanning until a matching element is found.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-large-array";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Create documents with large arrays (50 elements)
        StringBuilder largeArray1 = new StringBuilder("{'name': 'Data1', 'values': [");
        StringBuilder largeArray2 = new StringBuilder("{'name': 'Data2', 'values': [");
        StringBuilder largeArray3 = new StringBuilder("{'name': 'Data3', 'values': [");

        for (int i = 0; i < 50; i++) {
            if (i > 0) {
                largeArray1.append(", ");
                largeArray2.append(", ");
                largeArray3.append(", ");
            }
            largeArray1.append(i); // 0-49
            largeArray2.append(i + 50); // 50-99
            largeArray3.append(i * 2); // 0, 2, 4, ..., 98
        }
        largeArray1.append("]}");
        largeArray2.append("]}");
        largeArray3.append("]}");

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes(largeArray1.toString()),
                BSONUtil.jsonToDocumentThenBytes(largeArray2.toString()),
                BSONUtil.jsonToDocumentThenBytes(largeArray3.toString())
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find documents with at least one value > 90
        String query = "{'values': {'$elemMatch': {'$gt': 90}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Data1: max is 49 < 90
            // Data2: has values 91-99 > 90 ✓
            // Data3: max is 98 > 90 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Data2", "Data3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithMinimalConditionInsideElemMatch() {
        // Behavior: A simple boolean condition inside $elemMatch matches documents where at
        // least one array element has the specified field value.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-minimal";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc1', 'items': [{'active': true}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc2', 'items': [{'active': false}, {'active': true}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc3', 'items': [{'active': false}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc4', 'items': []}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: Find documents with at least one item where active = true
        String query = "{'items': {'$elemMatch': {'active': true}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Doc1", "Doc2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchScalarObjectIdArrayWithEq() {
        // Behavior: $elemMatch with $eq on scalar ObjectId arrays matches documents where at least
        // one ObjectId element exactly equals the specified value.

        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-objectid-eq";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        ObjectId oid3 = new ObjectId();
        ObjectId target = new ObjectId();

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc1', 'refs': [{\"$oid\": \"" + oid1.toHexString() + "\"}, {\"$oid\": \"" + target.toHexString() + "\"}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc2', 'refs': [{\"$oid\": \"" + oid2.toHexString() + "\"}, {\"$oid\": \"" + oid3.toHexString() + "\"}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc3', 'refs': [{\"$oid\": \"" + oid3.toHexString() + "\"}, {\"$oid\": \"" + oid1.toHexString() + "\"}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{'refs': {'$elemMatch': {'$eq': '" + target.toHexString() + "'}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Doc1"), extractNamesFromResults(results));
        }
    }
}
