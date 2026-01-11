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
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class NotOperatorIntegrationTest extends BasePipelineTest {

    @Test
    void shouldMatchDocumentsNotMatchingCondition() {
        final String TEST_BUCKET_NAME = "test-bucket-not-basic";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'status': 'inactive'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'status': 'pending'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find documents where status is NOT 'active'
        PipelineNode plan = createExecutionPlan(metadata, "{'status': {'$not': {'$eq': 'active'}}}");

        // $not always uses FullScanNode
        assertInstanceOf(FullScanNode.class, plan, "Should use FullScanNode for $not operator");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: status='active' - NOT matches = false
            // Bob: status='inactive' - NOT matches = true ✓
            // Charlie: status='active' - NOT matches = false
            // Diana: status='pending' - NOT matches = true ✓
            assertEquals(2, results.size(), "Should return 2 documents");
            assertEquals(Set.of("Bob", "Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchDocumentsNotGreaterThan() {
        final String TEST_BUCKET_NAME = "test-bucket-not-gt";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 20}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find documents where age is NOT greater than 25 (i.e., age <= 25)
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$not': {'$gt': 25}}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: age=25, NOT(25 > 25) = NOT(false) = true ✓
            // Bob: age=30, NOT(30 > 25) = NOT(true) = false
            // Charlie: age=35, NOT(35 > 25) = NOT(true) = false
            // Diana: age=20, NOT(20 > 25) = NOT(false) = true ✓
            assertEquals(2, results.size(), "Should return 2 documents");
            assertEquals(Set.of("Alice", "Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchDocumentsNotInList() {
        final String TEST_BUCKET_NAME = "test-bucket-not-in";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'role': 'admin'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'role': 'user'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'role': 'editor'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'role': 'admin'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find documents where role is NOT in ['admin', 'editor']
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$not': {'$in': ['admin', 'editor']}}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: role='admin' in list - NOT matches = false
            // Bob: role='user' not in list - NOT matches = true ✓
            // Charlie: role='editor' in list - NOT matches = false
            // Diana: role='admin' in list - NOT matches = false
            assertEquals(1, results.size(), "Should return 1 document");
            assertEquals(Set.of("Bob"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNotWithMissingField() {
        final String TEST_BUCKET_NAME = "test-bucket-not-missing";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'score': 85}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}"),  // No score field
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'score': 90}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'score': null}")  // score is null
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find documents where score is NOT >= 80
        // Documents with missing/null fields: the inner condition fails, so NOT succeeds
        PipelineNode plan = createExecutionPlan(metadata, "{'score': {'$not': {'$gte': 80}}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: score=85 >= 80, NOT matches = false
            // Bob: score missing, inner $gte fails, NOT matches = true ✓
            // Charlie: score=90 >= 80, NOT matches = false
            // Diana: score=null, inner $gte fails, NOT matches = true ✓
            assertEquals(2, results.size(), "Should return 2 documents");
            assertEquals(Set.of("Bob", "Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineNotWithAnd() {
        final String TEST_BUCKET_NAME = "test-bucket-not-and";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active', 'priority': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'status': 'inactive', 'priority': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'status': 'active', 'priority': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'status': 'pending', 'priority': 1}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find documents where status is NOT 'active' AND priority is 1
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$and': [{'status': {'$not': {'$eq': 'active'}}}, {'priority': 1}]}");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: NOT active=false, priority=1 -> false AND true = false
            // Bob: NOT active=true, priority=2 -> true AND false = false
            // Charlie: NOT active=false, priority=3 -> false AND false = false
            // Diana: NOT active=true, priority=1 -> true AND true = true ✓
            assertEquals(1, results.size(), "Should return 1 document");
            assertEquals(Set.of("Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNotInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-not-elemmatch";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'status': 'shipped'}, {'status': 'pending'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'status': 'cancelled'}, {'status': 'cancelled'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'status': 'delivered'}, {'status': 'shipped'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'status': 'pending'}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item where status is NOT 'cancelled'
        PipelineNode plan = createExecutionPlan(metadata,
                "{'items': {'$elemMatch': {'status': {'$not': {'$eq': 'cancelled'}}}}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: has 'shipped' and 'pending', neither is 'cancelled' ✓
            // Order2: both items are 'cancelled', no item NOT cancelled
            // Order3: has 'delivered' and 'shipped', neither is 'cancelled' ✓
            // Order4: has 'pending', not 'cancelled' ✓
            assertEquals(3, results.size(), "Should return 3 documents");
            assertEquals(Set.of("Order1", "Order3", "Order4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleDoubleNegation() {
        final String TEST_BUCKET_NAME = "test-bucket-not-double";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'active': true}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: NOT(NOT(active = true)) should be equivalent to (active = true)
        // The RemoveDoubleNotTransform should optimize this
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$not': {'$not': {'active': true}}}");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // After double negation elimination, should match active=true
            assertEquals(2, results.size(), "Should return 2 documents with active=true");
            assertEquals(Set.of("Alice", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNotWithNullValue() {
        final String TEST_BUCKET_NAME = "test-bucket-not-null";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'status': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie'}"),  // status field missing
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'status': 'inactive'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find documents where status is NOT null
        PipelineNode plan = createExecutionPlan(metadata, "{'status': {'$not': {'$eq': null}}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: status='active', NOT(status == null) = true ✓
            // Bob: status=null, NOT(status == null) = false
            // Charlie: status missing, treated as null for $eq null, NOT matches = false
            // Diana: status='inactive', NOT(status == null) = true ✓
            assertEquals(2, results.size(), "Should return 2 documents where status is not null");
            assertEquals(Set.of("Alice", "Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNotEqNullWithExplicitNullValues() {
        final String TEST_BUCKET_NAME = "test-bucket-not-eq-null";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'value': 100}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'value': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'value': 0}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana'}"),  // value field missing
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'value': null}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: NOT(value == null) - should exclude documents with null or missing value
        PipelineNode plan = createExecutionPlan(metadata, "{'value': {'$not': {'$eq': null}}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: value=100, NOT null = true ✓
            // Bob: value=null, NOT null = false
            // Charlie: value=0, NOT null = true ✓
            // Diana: value missing (treated as null), NOT null = false
            // Eve: value=null, NOT null = false
            assertEquals(2, results.size(), "Should return 2 documents with non-null values");
            assertEquals(Set.of("Alice", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleTopLevelNot() {
        final String TEST_BUCKET_NAME = "test-bucket-not-top-level";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'status': 'inactive'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'status': 'pending'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Top-level $not: {"$not": {"status": "active"}}
        // Should match documents where status is NOT 'active'
        PipelineNode plan = createExecutionPlan(metadata, "{'$not': {'status': 'active'}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // NOT(status == 'active') should return Bob, Diana
            assertEquals(2, results.size(), "Should return 2 documents");
            assertEquals(Set.of("Bob", "Diana"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNotWithExists() {
        final String TEST_BUCKET_NAME = "test-bucket-not-exists";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'email': 'alice@example.com'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}"),  // no email field
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'email': null}"),  // email is null
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'email': 'diana@example.com'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find documents where email field does NOT exist
        // {"email": {"$not": {"$exists": true}}} should be equivalent to {"email": {"$exists": false}}
        PipelineNode plan = createExecutionPlan(metadata, "{'email': {'$not': {'$exists': true}}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice: email exists = true, NOT exists = false
            // Bob: email missing, exists = false, NOT exists = true ✓
            // Charlie: email is null but field exists, exists = true, NOT exists = false
            // Diana: email exists = true, NOT exists = false
            assertEquals(1, results.size(), "Should return 1 document where email doesn't exist");
            assertEquals(Set.of("Bob"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNotWithNe() {
        final String TEST_BUCKET_NAME = "test-bucket-not-ne";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'role': 'admin'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'role': 'user'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'role': 'admin'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'role': 'guest'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: NOT(role != 'admin') should be equivalent to (role == 'admin')
        // Double negation: $not with $ne
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$not': {'$ne': 'admin'}}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // NOT(role != 'admin') = (role == 'admin')
            // Alice: role='admin', NOT(admin != admin) = NOT(false) = true ✓
            // Bob: role='user', NOT(user != admin) = NOT(true) = false
            // Charlie: role='admin', NOT(admin != admin) = NOT(false) = true ✓
            // Diana: role='guest', NOT(guest != admin) = NOT(true) = false
            assertEquals(2, results.size(), "Should return 2 documents with role='admin'");
            assertEquals(Set.of("Alice", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleMultipleOperatorsOnSameFieldWithNot() {
        final String TEST_BUCKET_NAME = "test-bucket-not-multi-op";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'age': 20}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: age NOT > 30 AND age < 40
        // This means: age <= 30 AND age < 40, which is age <= 30
        // Using separate conditions in $and to avoid parser issues
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$and': [{'age': {'$not': {'$gt': 30}}}, {'age': {'$lt': 40}}]}");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // NOT(age > 30) AND age < 40
            // Alice: NOT(25 > 30) AND 25 < 40 = true AND true = true ✓
            // Bob: NOT(30 > 30) AND 30 < 40 = true AND true = true ✓
            // Charlie: NOT(35 > 30) AND 35 < 40 = false AND true = false
            // Diana: NOT(40 > 30) AND 40 < 40 = false AND false = false
            // Eve: NOT(20 > 30) AND 20 < 40 = true AND true = true ✓
            assertEquals(3, results.size(), "Should return 3 documents");
            assertEquals(Set.of("Alice", "Bob", "Eve"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNotWithExistsFalse() {
        final String TEST_BUCKET_NAME = "test-bucket-not-exists-false";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'phone': '123-456'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}"),  // no phone
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'phone': '789-012'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana'}")  // no phone
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: NOT(phone doesn't exist) = phone exists
        PipelineNode plan = createExecutionPlan(metadata, "{'phone': {'$not': {'$exists': false}}}");
        assertInstanceOf(FullScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // NOT(phone doesn't exist) = phone exists
            // Alice: phone exists, NOT(false) = true ✓
            // Bob: phone missing, NOT(true) = false
            // Charlie: phone exists, NOT(false) = true ✓
            // Diana: phone missing, NOT(true) = false
            assertEquals(2, results.size(), "Should return 2 documents where phone exists");
            assertEquals(Set.of("Alice", "Charlie"), extractNamesFromResults(results));
        }
    }
}
