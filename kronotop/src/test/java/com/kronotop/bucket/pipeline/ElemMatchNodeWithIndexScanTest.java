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

/**
 * Tests for $elemMatch operator with index scans.
 *
 * <p><b>Note on ordering with multi-key indexes:</b> Result ordering is undefined when using
 * multi-key indexes on array fields. Each document creates multiple index entries (one per array
 * element), so the order in which documents are returned cannot be guaranteed. Therefore, these
 * tests verify correctness of results (matching documents) but do not test ordering behavior
 * with reverse=true on multi-key indexed arrays.</p>
 */
class ElemMatchNodeWithIndexScanTest extends BasePipelineTest {

    @Test
    void shouldCombineIndexScanWithElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-index-combined";

        // Create index on 'category' field
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex);

        // Insert documents with category and items array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'category': 'electronics', 'items': [{'price': 50}, {'price': 80}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'category': 'electronics', 'items': [{'price': 150}, {'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'category': 'books', 'items': [{'price': 150}, {'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'category': 'electronics', 'items': [{'price': 30}, {'price': 40}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: category = 'electronics' AND items has at least one with price > 100
        String query = "{'category': 'electronics', 'items': {'$elemMatch': {'price': {'$gt': 100}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: IndexScanNode -> TransformWithResidualPredicateNode (with predicates including elemMatch)
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");
        TransformWithResidualPredicateNode transform = (TransformWithResidualPredicateNode) plan.next();
        // Predicate can be ResidualAndNode when multiple predicates are merged
        assertInstanceOf(ResidualAndNode.class, transform.predicate(), "Predicate should be ResidualAndNode");
        ResidualAndNode andNode = (ResidualAndNode) transform.predicate();
        assertTrue(andNode.children().stream().anyMatch(p -> p instanceof ResidualElemMatchNode),
                "ResidualAndNode should contain a ResidualElemMatchNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Order2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineRangeScanWithElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-range-combined";

        // Create index on 'priority' field
        IndexDefinition priorityIndex = IndexDefinition.create("priority-index", "priority", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priorityIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'priority': 1, 'tags': [{'type': 'urgent'}, {'type': 'bug'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'priority': 5, 'tags': [{'type': 'feature'}, {'type': 'enhancement'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'priority': 3, 'tags': [{'type': 'urgent'}, {'type': 'feature'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task4', 'priority': 8, 'tags': [{'type': 'urgent'}, {'type': 'critical'}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: priority > 2 AND tags has at least one with type = 'urgent'
        String query = "{'priority': {'$gt': 2}, 'tags': {'$elemMatch': {'type': 'urgent'}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: IndexScanNode (for GT) -> TransformWithResidualPredicateNode (with predicates including elemMatch)
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");
        TransformWithResidualPredicateNode transform = (TransformWithResidualPredicateNode) plan.next();
        // Predicate can be ResidualAndNode when multiple predicates are merged
        assertInstanceOf(ResidualAndNode.class, transform.predicate(), "Predicate should be ResidualAndNode");
        ResidualAndNode andNode = (ResidualAndNode) transform.predicate();
        assertTrue(andNode.children().stream().anyMatch(p -> p instanceof ResidualElemMatchNode),
                "ResidualAndNode should contain a ResidualElemMatchNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Task3", "Task4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineMultipleIndexScansWithElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-multi-index";

        // Create indexes on 'status' and 'region' fields
        IndexDefinition statusIndex = IndexDefinition.create("status-index", "status", BsonType.STRING);
        IndexDefinition regionIndex = IndexDefinition.create("region-index", "region", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex, regionIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store1', 'status': 'active', 'region': 'US', 'products': [{'stock': 100}, {'stock': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store2', 'status': 'active', 'region': 'EU', 'products': [{'stock': 50}, {'stock': 30}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store3', 'status': 'active', 'region': 'US', 'products': [{'stock': 10}, {'stock': 5}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store4', 'status': 'inactive', 'region': 'US', 'products': [{'stock': 500}, {'stock': 600}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: status = 'active' AND region = 'US' AND products has at least one with stock >= 100
        String query = "{'status': 'active', 'region': 'US', 'products': {'$elemMatch': {'stock': {'$gte': 100}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: IndexScanNode -> TransformWithResidualPredicateNode (merged predicates including elemMatch)
        // Multiple index scans become PhysicalIndexIntersection, then one IndexScan with residual predicates
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");
        // The predicate should be ResidualAndNode containing the other index condition and elemMatch
        TransformWithResidualPredicateNode transform = (TransformWithResidualPredicateNode) plan.next();
        assertInstanceOf(ResidualAndNode.class, transform.predicate(), "Predicate should be ResidualAndNode (merged)");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Store1"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleOrWithIndexScanAndElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-or-index";

        // Create index on 'type' field
        IndexDefinition typeIndex = IndexDefinition.create("type-index", "type", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, typeIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc1', 'type': 'premium', 'features': [{'enabled': true}, {'enabled': false}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc2', 'type': 'basic', 'features': [{'enabled': true}, {'enabled': true}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc3', 'type': 'premium', 'features': [{'enabled': false}, {'enabled': false}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc4', 'type': 'enterprise', 'features': [{'enabled': true}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: type = 'premium' OR (type = 'basic' AND features has enabled = true)
        String query = "{'$or': [{'type': 'premium'}, {'type': 'basic', 'features': {'$elemMatch': {'enabled': true}}}]}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: UnionNode with two branches (index scan for premium, index scan + elemMatch for basic)
        assertInstanceOf(UnionNode.class, plan, "Root should be UnionNode for OR query");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size(), "Should return exactly 3 documents");
            assertEquals(Set.of("Doc1", "Doc2", "Doc3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleElemMatchWithMultipleConditionsAndIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-multi-cond-index";

        // Create index on 'department' field
        IndexDefinition deptIndex = IndexDefinition.create("dept-index", "department", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, deptIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Team1', 'department': 'engineering', 'members': [{'role': 'developer', 'level': 3}, {'role': 'manager', 'level': 5}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Team2', 'department': 'engineering', 'members': [{'role': 'developer', 'level': 5}, {'role': 'developer', 'level': 2}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Team3', 'department': 'marketing', 'members': [{'role': 'developer', 'level': 5}, {'role': 'analyst', 'level': 4}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Team4', 'department': 'engineering', 'members': [{'role': 'analyst', 'level': 5}, {'role': 'manager', 'level': 4}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: department = 'engineering' AND members has at least one developer with level >= 5
        String query = "{'department': 'engineering', 'members': {'$elemMatch': {'role': 'developer', 'level': {'$gte': 5}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: IndexScanNode -> TransformWithResidualPredicateNode (with predicates including elemMatch)
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");
        TransformWithResidualPredicateNode transform = (TransformWithResidualPredicateNode) plan.next();
        // Predicate can be ResidualAndNode when multiple predicates are merged
        assertInstanceOf(ResidualAndNode.class, transform.predicate(), "Predicate should be ResidualAndNode");
        ResidualAndNode andNode = (ResidualAndNode) transform.predicate();
        assertTrue(andNode.children().stream().anyMatch(p -> p instanceof ResidualElemMatchNode),
                "ResidualAndNode should contain a ResidualElemMatchNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Team2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleIndexScanWithNoMatchingElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-no-match-index";

        // Create index on 'status' field
        IndexDefinition statusIndex = IndexDefinition.create("status-index", "status", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'status': 'pending', 'items': [{'qty': 5}, {'qty': 10}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'status': 'pending', 'items': [{'qty': 3}, {'qty': 7}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: status = 'pending' AND items has qty > 100 (no match)
        String query = "{'status': 'pending', 'items': {'$elemMatch': {'qty': {'$gt': 100}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: IndexScanNode -> TransformWithResidualPredicateNode (with predicates including elemMatch)
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");
        TransformWithResidualPredicateNode transform = (TransformWithResidualPredicateNode) plan.next();
        // Predicate can be ResidualAndNode when multiple predicates are merged
        assertInstanceOf(ResidualAndNode.class, transform.predicate(), "Predicate should be ResidualAndNode");
        ResidualAndNode andNode = (ResidualAndNode) transform.predicate();
        assertTrue(andNode.children().stream().anyMatch(p -> p instanceof ResidualElemMatchNode),
                "ResidualAndNode should contain a ResidualElemMatchNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertTrue(results.isEmpty(), "Should return no documents");
        }
    }

    @Test
    void shouldHandleInOperatorWithElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-in-operator";

        // Create index on 'category' field
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'category': 'electronics', 'reviews': [{'rating': 5}, {'rating': 4}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'category': 'clothing', 'reviews': [{'rating': 3}, {'rating': 2}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'category': 'books', 'reviews': [{'rating': 5}, {'rating': 5}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'category': 'electronics', 'reviews': [{'rating': 2}, {'rating': 1}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: category in ['electronics', 'books'] AND reviews has at least one with rating = 5
        String query = "{'category': {'$in': ['electronics', 'books']}, 'reviews': {'$elemMatch': {'rating': 5}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: UnionNode (for $in with multiple values), each branch with elemMatch filter
        assertInstanceOf(UnionNode.class, plan, "Root should be UnionNode for $in with multiple values");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product1", "Product3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNestedElemMatchConditionsWithIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-nested-index";

        // Create index on 'active' field
        IndexDefinition activeIndex = IndexDefinition.create("active-index", "active", BsonType.BOOLEAN);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, activeIndex);

        // Insert documents with a nested structure
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company1', 'active': true, 'departments': [{'budget': {'amount': 50000}}, {'budget': {'amount': 100000}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company2', 'active': true, 'departments': [{'budget': {'amount': 20000}}, {'budget': {'amount': 30000}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company3', 'active': false, 'departments': [{'budget': {'amount': 200000}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company4', 'active': true, 'departments': [{'budget': {'amount': 150000}}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: active = true AND departments have at least one with budget.amount >= 100000
        String query = "{'active': true, 'departments': {'$elemMatch': {'budget.amount': {'$gte': 100000}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: IndexScanNode -> TransformWithResidualPredicateNode (with predicates including elemMatch)
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");
        TransformWithResidualPredicateNode transform = (TransformWithResidualPredicateNode) plan.next();
        // Predicate can be ResidualAndNode when multiple predicates are merged
        assertInstanceOf(ResidualAndNode.class, transform.predicate(), "Predicate should be ResidualAndNode");
        ResidualAndNode andNode = (ResidualAndNode) transform.predicate();
        assertTrue(andNode.children().stream().anyMatch(p -> p instanceof ResidualElemMatchNode),
                "ResidualAndNode should contain a ResidualElemMatchNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Company1", "Company4"), extractNamesFromResults(results));
        }
    }

    // Scalar Array Tests with Index Scan

    @Test
    void shouldCombineIndexScanWithScalarStringArrayElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-string-index";

        // Create index on 'status' field
        IndexDefinition statusIndex = IndexDefinition.create("status-index", "status", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex);

        // Insert documents with scalar string array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'status': 'open', 'tags': ['bug', 'feature', 'enhancement']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'status': 'open', 'tags': ['urgent', 'bug', 'critical']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'status': 'closed', 'tags': ['urgent', 'documentation']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task4', 'status': 'open', 'tags': ['documentation', 'help-wanted']}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: status = 'open' AND tags has at least one 'urgent' element
        String query = "{'status': 'open', 'tags': {'$elemMatch': {'$eq': 'urgent'}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Task2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineIndexScanWithScalarNumberArrayElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-number-index";

        // Create index on 'grade' field
        IndexDefinition gradeIndex = IndexDefinition.create("grade-index", "grade", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, gradeIndex);

        // Insert documents with scalar number array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student1', 'grade': 'A', 'scores': [75, 82, 88]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student2', 'grade': 'A', 'scores': [91, 95, 89]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student3', 'grade': 'B', 'scores': [92, 94, 96]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student4', 'grade': 'A', 'scores': [65, 70, 72]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: grade = 'A' AND scores has at least one score > 90
        String query = "{'grade': 'A', 'scores': {'$elemMatch': {'$gt': 90}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Student2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineRangeScanWithScalarArrayElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-range-index";

        // Create index on 'priority' field
        IndexDefinition priorityIndex = IndexDefinition.create("priority-index", "priority", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priorityIndex);

        // Insert documents with scalar number array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Project1', 'priority': 1, 'milestones': [10, 20, 30]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Project2', 'priority': 5, 'milestones': [50, 60, 70]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Project3', 'priority': 3, 'milestones': [100, 200, 300]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Project4', 'priority': 8, 'milestones': [150, 250, 350]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: priority > 2 AND milestones has at least one >= 100
        String query = "{'priority': {'$gt': 2}, 'milestones': {'$elemMatch': {'$gte': 100}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Project3", "Project4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineInOperatorWithScalarArrayElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-in-index";

        // Create index on 'category' field
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex);

        // Insert documents with scalar string array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item1', 'category': 'electronics', 'labels': ['sale', 'new', 'featured']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item2', 'category': 'clothing', 'labels': ['clearance', 'sale']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item3', 'category': 'books', 'labels': ['bestseller', 'new']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Item4', 'category': 'electronics', 'labels': ['refurbished', 'discount']}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: category in ['electronics', 'books'] AND labels has at least one 'new'
        String query = "{'category': {'$in': ['electronics', 'books']}, 'labels': {'$elemMatch': {'$eq': 'new'}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: UnionNode for $in
        assertInstanceOf(UnionNode.class, plan, "Root should be UnionNode for $in");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Item1", "Item3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineMultipleIndexesWithScalarArrayElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-multi-index";

        // Create indexes on 'status' and 'region' fields
        IndexDefinition statusIndex = IndexDefinition.create("status-index", "status", BsonType.STRING);
        IndexDefinition regionIndex = IndexDefinition.create("region-index", "region", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex, regionIndex);

        // Insert documents with scalar number array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Server1', 'status': 'active', 'region': 'US', 'ports': [80, 443, 8080]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Server2', 'status': 'active', 'region': 'EU', 'ports': [22, 80]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Server3', 'status': 'active', 'region': 'US', 'ports': [22, 3306]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Server4', 'status': 'inactive', 'region': 'US', 'ports': [80, 443, 8443]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: status = 'active' AND region = 'US' AND ports has at least one port = 443
        String query = "{'status': 'active', 'region': 'US', 'ports': {'$elemMatch': {'$eq': 443}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Server1"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleScalarArrayElemMatchWithRangeConditions() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-range-cond";

        // Create index on 'type' field
        IndexDefinition typeIndex = IndexDefinition.create("type-index", "type", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, typeIndex);

        // Insert documents with scalar number array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Batch1', 'type': 'production', 'temperatures': [45, 52, 48]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Batch2', 'type': 'production', 'temperatures': [78, 82, 85]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Batch3', 'type': 'testing', 'temperatures': [80, 85, 90]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Batch4', 'type': 'production', 'temperatures': [60, 65, 70]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: type = 'production' AND temperatures has at least one in range [75, 90]
        String query = "{'type': 'production', 'temperatures': {'$elemMatch': {'$gte': 75, '$lte': 90}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Batch2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleScalarArrayElemMatchNoMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-no-match";

        // Create index on 'active' field
        IndexDefinition activeIndex = IndexDefinition.create("active-index", "active", BsonType.BOOLEAN);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, activeIndex);

        // Insert documents with scalar number array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config1', 'active': true, 'values': [10, 20, 30]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config2', 'active': true, 'values': [40, 50, 60]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: active = true AND values has at least one > 100 (no match)
        String query = "{'active': true, 'values': {'$elemMatch': {'$gt': 100}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertTrue(results.isEmpty(), "Should return no documents");
        }
    }

    // Scalar Array with Index on Array Field Tests

    @Test
    void shouldUseIndexOnScalarNumberArray() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-array-indexed";

        // Create index on the scalar array field 'temperatures' (multikey)
        IndexDefinition temperaturesIndex = IndexDefinition.create("temperatures-index", "temperatures", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, temperaturesIndex);

        // Insert documents with scalar number array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Batch1', 'temperatures': [45, 52, 48]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Batch2', 'temperatures': [78, 82, 85]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Batch3', 'temperatures': [80, 85, 90]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Batch4', 'temperatures': [60, 65, 70]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: temperatures has at least one >= 80
        String query = "{'temperatures': {'$elemMatch': {'$gte': 80}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Batch2", "Batch3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseIndexOnScalarStringArray() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-string-array-indexed";

        // Create index on the scalar array field 'tags' (multikey)
        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        // Insert documents with scalar string array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Issue1', 'tags': ['bug', 'frontend', 'css']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Issue2', 'tags': ['feature', 'backend', 'api']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Issue3', 'tags': ['bug', 'backend', 'database']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Issue4', 'tags': ['documentation', 'help-wanted']}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: tags has at least one 'bug'
        String query = "{'tags': {'$elemMatch': {'$eq': 'bug'}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Issue1", "Issue3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineArrayIndexWithFieldIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-array-and-field-indexed";

        // Create index on both a regular field and the scalar array field (multikey)
        IndexDefinition typeIndex = IndexDefinition.create("type-index", "type", BsonType.STRING);
        IndexDefinition scoresIndex = IndexDefinition.create("scores-index", "scores", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, typeIndex, scoresIndex);

        // Insert documents with scalar number array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Exam1', 'type': 'midterm', 'scores': [75, 82, 88]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Exam2', 'type': 'final', 'scores': [91, 95, 89]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Exam3', 'type': 'midterm', 'scores': [92, 94, 96]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Exam4', 'type': 'final', 'scores': [65, 70, 72]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: type = 'midterm' AND scores has at least one > 90
        String query = "{'type': 'midterm', 'scores': {'$elemMatch': {'$gt': 90}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Exam3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseIndexOnScalarArrayWithRangeQuery() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-array-range-indexed";

        // Create index on the scalar array field 'prices' (multikey)
        IndexDefinition pricesIndex = IndexDefinition.create("prices-index", "prices", BsonType.DOUBLE, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, pricesIndex);

        // Insert documents with scalar number array (doubles)
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'prices': [9.99, 14.99, 19.99]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'prices': [29.99, 39.99, 49.99]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'prices': [99.99, 149.99, 199.99]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'prices': [5.99, 7.99, 8.99]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: prices has at least one in range [10.0, 50.0]
        String query = "{'prices': {'$elemMatch': {'$gte': 10.0, '$lte': 50.0}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify plan structure: RangeScanNode (consolidated from GTE + LTE) -> TransformWithResidualPredicateNode (for elemMatch)
        assertInstanceOf(RangeScanNode.class, plan, "Root should be RangeScanNode for indexed array field with range query");
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next(), "Next should be TransformWithResidualPredicateNode");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product1", "Product2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleIndexedScalarArrayWithNoMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-array-indexed-no-match";

        // Create index on the scalar array field 'values' (multikey)
        IndexDefinition valuesIndex = IndexDefinition.create("values-index", "values", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valuesIndex);

        // Insert documents with scalar number array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data1', 'values': [10, 20, 30]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data2', 'values': [40, 50, 60]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: values has at least one > 100 (no match)
        String query = "{'values': {'$elemMatch': {'$gt': 100}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertTrue(results.isEmpty(), "Should return no documents");
        }
    }

    @Test
    void shouldHandleOrWithMultipleElemMatchConditions() {
        final String TEST_BUCKET_NAME = "test-bucket-or-with-elemmatch";

        // Create index on the scalar array field (multikey)
        IndexDefinition scoresIndex = IndexDefinition.create("scores-index", "scores", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoresIndex);

        // Insert documents with scalar number array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student1', 'scores': [85, 90, 78]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student2', 'scores': [45, 55, 60]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student3', 'scores': [92, 88, 95]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student4', 'scores': [70, 72, 68]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: scores has at least one < 50 OR scores has at least one > 90
        // This uses $or at top level with two separate $elemMatch conditions
        String query = "{'$or': [{'scores': {'$elemMatch': {'$lt': 50}}}, {'scores': {'$elemMatch': {'$gt': 90}}}]}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Student1: 85, 90, 78 - no element < 50, no element > 90 -> no match
            // Student2: 45, 55, 60 - 45 < 50 -> match
            // Student3: 92, 88, 95 - 92 > 90 and 95 > 90 -> match
            // Student4: 70, 72, 68 - no element < 50 or > 90 -> no match
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Student2", "Student3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleMultipleElemMatchOnDifferentArrays() {
        final String TEST_BUCKET_NAME = "test-bucket-multiple-elemmatch";

        // Create indexes on two different array fields (multiKey for arrays)
        IndexDefinition scoresIndex = IndexDefinition.create("scores-index", "scores", BsonType.INT32, true);
        IndexDefinition ratingsIndex = IndexDefinition.create("ratings-index", "ratings", BsonType.DOUBLE, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoresIndex, ratingsIndex);

        // Insert documents with two scalar arrays
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'scores': [80, 85, 90], 'ratings': [4.5, 4.8, 4.2]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'scores': [70, 75, 72], 'ratings': [4.9, 4.7, 4.6]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'scores': [88, 92, 95], 'ratings': [3.5, 3.8, 3.2]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'scores': [60, 65, 58], 'ratings': [4.0, 4.1, 3.9]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: scores has at least one >= 85 AND ratings has at least one >= 4.5
        String query = "{'scores': {'$elemMatch': {'$gte': 85}}, 'ratings': {'$elemMatch': {'$gte': 4.5}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Product1: scores has 85, 90 >= 85; ratings has 4.5, 4.8 >= 4.5 -> match
            // Product2: scores max is 75 < 85 -> no match
            // Product3: scores has 88, 92, 95 >= 85; ratings max is 3.8 < 4.5 -> no match
            // Product4: scores max is 65 < 85 -> no match
            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Product1"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleMultipleElemMatchWithRangeQueries() {
        final String TEST_BUCKET_NAME = "test-bucket-multiple-elemmatch-range";

        // Create indexes on two different array fields (multiKey for arrays)
        IndexDefinition pricesIndex = IndexDefinition.create("prices-index", "prices", BsonType.DOUBLE, true);
        IndexDefinition quantitiesIndex = IndexDefinition.create("quantities-index", "quantities", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, pricesIndex, quantitiesIndex);

        // Insert documents with two scalar arrays
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'prices': [10.0, 25.0, 50.0], 'quantities': [5, 10, 15]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'prices': [100.0, 150.0, 200.0], 'quantities': [2, 3, 4]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'prices': [15.0, 30.0, 45.0], 'quantities': [8, 12, 20]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'prices': [5.0, 8.0, 12.0], 'quantities': [1, 2, 3]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: prices have at least one in [20, 60] AND quantities has at least one in [10, 25]
        String query = "{'prices': {'$elemMatch': {'$gte': 20.0, '$lte': 60.0}}, 'quantities': {'$elemMatch': {'$gte': 10, '$lte': 25}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: prices has 25, 50 in [20,60]; quantities has 10, 15 in [10,25] -> match
            // Order2: prices min is 100 > 60 -> no match
            // Order3: prices has 30, 45 in [20,60]; quantities has 12, 20 in [10,25] -> match
            // Order4: prices max is 12 < 20 -> no match
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order1", "Order3"), extractNamesFromResults(results));
        }
    }

    // $in inside $elemMatch with indexed arrays

    @Test
    void shouldUseIndexWithInOperatorInsideElemMatchOnDocumentArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-in-doc-indexed";

        // Create index on nested field inside array of documents (multiKey for arrays)
        IndexDefinition statusIndex = IndexDefinition.create("items-status-index", "items.status", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'status': 'pending'}, {'status': 'processing'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'status': 'shipped'}, {'status': 'delivered'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'status': 'cancelled'}, {'status': 'refunded'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'status': 'pending'}, {'status': 'shipped'}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item with status in ['shipped', 'delivered']
        // Index on items.status should be used; multi-value $in transforms to UnionNode
        String query = "{'items': {'$elemMatch': {'status': {'$in': ['shipped', 'delivered']}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);
        assertInstanceOf(UnionNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order2", "Order4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseIndexWithInOperatorInsideElemMatchOnScalarArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-in-indexed";

        // Create index on scalar array field (multiKey for arrays)
        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'tags': ['bug', 'feature', 'enhancement']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'tags': ['urgent', 'critical', 'blocker']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'tags': ['documentation', 'help-wanted']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task4', 'tags': ['bug', 'urgent']}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one tag in ['urgent', 'critical']
        // Index should be used for the $in lookup
        String query = "{'tags': {'$elemMatch': {'$in': ['urgent', 'critical']}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);
        assertInstanceOf(UnionNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Task2", "Task4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseIndexWithInOperatorInsideElemMatchOnNumberArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-in-indexed-numbers";

        // Create index on scalar number array field (multiKey for arrays)
        IndexDefinition scoresIndex = IndexDefinition.create("scores-index", "scores", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoresIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student1', 'scores': [75, 82, 88]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student2', 'scores': [90, 95, 100]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student3', 'scores': [60, 65, 70]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student4', 'scores': [85, 90, 92]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find students with at least one score in [90, 95, 100]
        String query = "{'scores': {'$elemMatch': {'$in': [90, 95, 100]}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);
        assertInstanceOf(UnionNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Student2", "Student4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineInWithOtherConditionsInsideElemMatchWithIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-in-combined-indexed";

        // Create index on scalar number array field (multiKey for arrays)
        IndexDefinition pricesIndex = IndexDefinition.create("prices-index", "prices", BsonType.DOUBLE, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, pricesIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'prices': [9.99, 19.99, 29.99]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'prices': [49.99, 59.99, 69.99]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'prices': [19.99, 39.99, 59.99]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'prices': [99.99, 149.99, 199.99]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find products with at least one price in [19.99, 59.99]
        String query = "{'prices': {'$elemMatch': {'$in': [19.99, 59.99]}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);
        assertInstanceOf(UnionNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Product1 has 19.99, Product2 has 59.99, Product3 has both
            assertEquals(3, results.size(), "Should return exactly 3 documents");
            assertEquals(Set.of("Product1", "Product2", "Product3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleInWithNoMatchOnIndexedArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-in-no-match-indexed";

        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'tags': ['bug', 'feature']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'tags': ['enhancement', 'improvement']}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one tag in ['nonexistent', 'missing']
        String query = "{'tags': {'$elemMatch': {'$in': ['nonexistent', 'missing']}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);
        assertInstanceOf(UnionNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertTrue(results.isEmpty(), "Should return no documents");
        }
    }

    @Test
    void shouldCombineIndexedElemMatchInWithOtherIndexedCondition() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-in-multi-index";

        // Create indexes on an array field (multiKey) and a regular field
        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        IndexDefinition statusIndex = IndexDefinition.create("status-index", "status", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'status': 'open', 'tags': ['bug', 'urgent']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'status': 'closed', 'tags': ['bug', 'critical']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'status': 'open', 'tags': ['feature', 'enhancement']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task4', 'status': 'open', 'tags': ['critical', 'blocker']}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find open tasks with at least one tag in ['critical', 'blocker']
        String query = "{'status': 'open', 'tags': {'$elemMatch': {'$in': ['critical', 'blocker']}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);
        assertInstanceOf(IndexScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Task1 has 'urgent' not in the list, Task2 is closed, Task3 has neither, Task4 matches
            assertEquals(1, results.size(), "Should return exactly 1 document");
            assertEquals(Set.of("Task4"), extractNamesFromResults(results));
        }
    }

    // Explicit $or inside $elemMatch with indexes

    @Test
    void shouldUseIndexWithOrInsideElemMatchOnDocumentArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-or-doc-indexed";

        // Create index on nested field inside array of documents (multiKey for arrays)
        IndexDefinition subjectIndex = IndexDefinition.create("scores-subject-index", "scores.subject", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, subjectIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student1', 'scores': [{'subject': 'math', 'score': 85}, {'subject': 'history', 'score': 75}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student2', 'scores': [{'subject': 'science', 'score': 90}, {'subject': 'art', 'score': 60}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student3', 'scores': [{'subject': 'english', 'score': 88}, {'subject': 'music', 'score': 70}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student4', 'scores': [{'subject': 'math', 'score': 92}, {'subject': 'science', 'score': 85}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find students with at least one score where subject is 'math' OR 'science'
        // Index on scores.subject should be used
        String query = "{'scores': {'$elemMatch': {'$or': [{'subject': 'math'}, {'subject': 'science'}]}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // $or with two indexed conditions becomes UnionNode
        assertInstanceOf(UnionNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Student1: has math ✓
            // Student2: has science ✓
            // Student3: no math/science
            // Student4: has both math and science ✓
            assertEquals(3, results.size(), "Should return exactly 3 documents");
            assertEquals(Set.of("Student1", "Student2", "Student4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseIndexWithOrInsideElemMatchOnScalarArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-or-scalar-indexed";

        // Create index on scalar array field (multiKey for arrays)
        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'tags': ['bug', 'critical']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'tags': ['feature', 'enhancement']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'tags': ['urgent', 'bug']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task4', 'tags': ['documentation', 'help-wanted']}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one tag that is 'critical' OR 'urgent'
        // Index on tags should be used
        String query = "{'tags': {'$elemMatch': {'$or': [{'$eq': 'critical'}, {'$eq': 'urgent'}]}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // $or with two indexed conditions becomes UnionNode
        assertInstanceOf(UnionNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Task1", "Task3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineOrWithOtherConditionsInsideElemMatchWithIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-or-combined-indexed";

        // Create indexes on nested fields (multiKey for arrays)
        IndexDefinition subjectIndex = IndexDefinition.create("scores-subject-index", "scores.subject", BsonType.STRING, true);
        IndexDefinition scoreIndex = IndexDefinition.create("scores-score-index", "scores.score", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, subjectIndex, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student1', 'scores': [{'subject': 'math', 'score': 85}, {'subject': 'history', 'score': 75}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student2', 'scores': [{'subject': 'science', 'score': 90}, {'subject': 'art', 'score': 60}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student3', 'scores': [{'subject': 'math', 'score': 70}, {'subject': 'music', 'score': 95}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Student4', 'scores': [{'subject': 'math', 'score': 92}, {'subject': 'science', 'score': 85}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find students with at least one score where (subject is 'math' OR 'science') AND score > 80
        String query = "{'scores': {'$elemMatch': {'$or': [{'subject': 'math'}, {'subject': 'science'}], 'score': {'$gt': 80}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        assertInstanceOf(IndexScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Student1: math=85 > 80 ✓
            // Student2: science=90 > 80 ✓
            // Student3: math=70 < 80 - no match
            // Student4: math=92 > 80 ✓, science=85 > 80 ✓
            assertEquals(3, results.size(), "Should return exactly 3 documents");
            assertEquals(Set.of("Student1", "Student2", "Student4"), extractNamesFromResults(results));
        }
    }

    // $all inside $elemMatch with indexes

    @Test
    void shouldUseIndexWithAllInsideElemMatchOnDocumentArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-all-indexed";

        // Create index on nested field (multiKey for arrays) - note: $all on array field cannot directly use index
        // but other conditions in the same $elemMatch can use indexes
        IndexDefinition priceIndex = IndexDefinition.create("items-price-index", "items.price", BsonType.DOUBLE, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'categories': ['electronics', 'sale'], 'price': 100.0}, {'categories': ['books'], 'price': 20.0}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'categories': ['electronics', 'sale'], 'price': 500.0}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'categories': ['electronics'], 'price': 150.0}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'categories': ['electronics', 'sale', 'clearance'], 'price': 75.0}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item that has ALL of ['electronics', 'sale'] AND price < 200
        // The price condition can use the index
        String query = "{'items': {'$elemMatch': {'categories': {'$all': ['electronics', 'sale']}, 'price': {'$lt': 200.0}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: first item has both categories AND price=100 < 200 ✓
            // Order2: has both categories BUT price=500 >= 200
            // Order3: missing 'sale' category
            // Order4: has both categories AND price=75 < 200 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order1", "Order4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineAllWithIndexedConditionInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-all-combined-indexed";

        // Create index on status field (multiKey for arrays)
        IndexDefinition statusIndex = IndexDefinition.create("items-status-index", "items.status", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'items': [{'tags': ['featured', 'new'], 'status': 'active'}, {'tags': ['sale'], 'status': 'inactive'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'items': [{'tags': ['featured', 'new'], 'status': 'inactive'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'items': [{'tags': ['featured'], 'status': 'active'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'items': [{'tags': ['featured', 'new', 'premium'], 'status': 'active'}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find products with at least one item that has ALL of ['featured', 'new'] AND status='active'
        // status condition uses index
        String query = "{'items': {'$elemMatch': {'tags': {'$all': ['featured', 'new']}, 'status': 'active'}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Should use index for status='active'
        assertInstanceOf(IndexScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Product1: first item has both tags AND active ✓
            // Product2: has both tags but inactive
            // Product3: missing 'new' tag
            // Product4: has both tags AND active ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product1", "Product4"), extractNamesFromResults(results));
        }
    }

    // $size inside $elemMatch with indexes

    @Test
    void shouldCombineSizeWithIndexedConditionInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-size-indexed";

        // Create index on priority field (multiKey for arrays)
        IndexDefinition priorityIndex = IndexDefinition.create("tasks-priority-index", "tasks.priority", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priorityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Project1', 'tasks': [{'assignees': ['alice', 'bob'], 'priority': 'high'}, {'assignees': ['charlie'], 'priority': 'low'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Project2', 'tasks': [{'assignees': ['dave', 'eve'], 'priority': 'low'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Project3', 'tasks': [{'assignees': ['frank', 'grace'], 'priority': 'high'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Project4', 'tasks': [{'assignees': ['helen'], 'priority': 'high'}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find projects with at least one task that has exactly 2 assignees AND high priority
        // priority condition uses index
        String query = "{'tasks': {'$elemMatch': {'assignees': {'$size': 2}, 'priority': 'high'}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Should use index for priority='high'
        assertInstanceOf(IndexScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Project1: first task has 2 assignees AND high priority ✓
            // Project2: has 2 assignees but low priority
            // Project3: has 2 assignees AND high priority ✓
            // Project4: has 1 assignee (not 2)
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Project1", "Project3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineSizeWithRangeIndexInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-size-range-indexed";

        // Create index on count field (multiKey for arrays)
        IndexDefinition countIndex = IndexDefinition.create("entries-count-index", "entries.count", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, countIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record1', 'entries': [{'values': [1, 2, 3], 'count': 100}, {'values': [4, 5], 'count': 50}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record2', 'entries': [{'values': [1, 2, 3], 'count': 30}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record3', 'entries': [{'values': [1, 2], 'count': 80}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record4', 'entries': [{'values': [1, 2, 3], 'count': 90}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find records with at least one entry that has exactly 3 values AND count > 50
        // count condition uses index
        String query = "{'entries': {'$elemMatch': {'values': {'$size': 3}, 'count': {'$gt': 50}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Record1: first entry has 3 values AND count=100 > 50 ✓
            // Record2: has 3 values but count=30 <= 50
            // Record3: has 2 values (not 3)
            // Record4: has 3 values AND count=90 > 50 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Record1", "Record4"), extractNamesFromResults(results));
        }
    }

    // Nested $elemMatch with indexes

    @Test
    void shouldUseIndexWithNestedElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-elemmatch-indexed";

        // Create index on nested price field (multiKey for arrays)
        IndexDefinition priceIndex = IndexDefinition.create("products-price-index", "departments.products.price", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store1', 'departments': [{'name': 'Electronics', 'products': [{'sku': 'TV1', 'price': 500}, {'sku': 'TV2', 'price': 800}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store2', 'departments': [{'name': 'Electronics', 'products': [{'sku': 'PC1', 'price': 1200}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store3', 'departments': [{'name': 'Food', 'products': [{'sku': 'F1', 'price': 10}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store4', 'departments': [{'name': 'Electronics', 'products': [{'sku': 'CAM1', 'price': 450}]}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find stores with a department that has a product with price > 600
        String query = "{'departments': {'$elemMatch': {'products': {'$elemMatch': {'price': {'$gt': 600}}}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Store1: TV2=800 > 600 ✓
            // Store2: PC1=1200 > 600 ✓
            // Store3: F1=10 < 600
            // Store4: CAM1=450 < 600
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Store1", "Store2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineNestedElemMatchWithIndexedOuterCondition() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-elemmatch-outer-indexed";

        // Create index on department name (multiKey for arrays)
        IndexDefinition nameIndex = IndexDefinition.create("dept-name-index", "departments.name", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, nameIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store1', 'departments': [{'name': 'Electronics', 'products': [{'price': 500}, {'price': 800}]}, {'name': 'Books', 'products': [{'price': 20}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store2', 'departments': [{'name': 'Electronics', 'products': [{'price': 100}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store3', 'departments': [{'name': 'Clothing', 'products': [{'price': 600}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store4', 'departments': [{'name': 'Electronics', 'products': [{'price': 700}]}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find stores with an Electronics department that has a product with price > 400
        // The 'name' condition can use the index
        String query = "{'departments': {'$elemMatch': {'name': 'Electronics', 'products': {'$elemMatch': {'price': {'$gt': 400}}}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Should use index for name='Electronics'
        assertInstanceOf(IndexScanNode.class, plan);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Store1: Electronics dept has products 500, 800 > 400 ✓
            // Store2: Electronics dept has product 100 < 400
            // Store3: Clothing dept (not Electronics)
            // Store4: Electronics dept has product 700 > 400 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Store1", "Store4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldHandleNestedElemMatchWithMultipleIndexes() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-elemmatch-multi-indexed";

        // Create multiple indexes (multiKey for arrays)
        IndexDefinition teamNameIndex = IndexDefinition.create("team-name-index", "teams.name", BsonType.STRING, true);
        IndexDefinition memberRoleIndex = IndexDefinition.create("member-role-index", "teams.members.role", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, teamNameIndex, memberRoleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company1', 'teams': [{'name': 'Engineering', 'members': [{'role': 'engineer', 'level': 5}, {'role': 'engineer', 'level': 3}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company2', 'teams': [{'name': 'Engineering', 'members': [{'role': 'manager', 'level': 4}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company3', 'teams': [{'name': 'Sales', 'members': [{'role': 'engineer', 'level': 4}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Company4', 'teams': [{'name': 'Engineering', 'members': [{'role': 'engineer', 'level': 4}]}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find companies with an Engineering team that has an engineer with level >= 4
        String query = "{'teams': {'$elemMatch': {'name': 'Engineering', 'members': {'$elemMatch': {'role': 'engineer', 'level': {'$gte': 4}}}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Company1: Engineering team has engineer level 5 >= 4 ✓
            // Company2: Engineering team has no engineers
            // Company3: Sales team (not Engineering)
            // Company4: Engineering team has engineer level 4 >= 4 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Company1", "Company4"), extractNamesFromResults(results));
        }
    }

    // Medium Priority: Multiple $elemMatch on same field with indexes

    @Test
    void shouldCombineMultipleElemMatchOnSameFieldWithIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-multi-elemmatch-same-field-indexed";

        // Create index on items.price (multiKey for arrays)
        IndexDefinition priceIndex = IndexDefinition.create("items-price-index", "items.price", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'price': 50, 'qty': 2}, {'price': 150, 'qty': 5}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'price': 200, 'qty': 1}, {'price': 30, 'qty': 10}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'price': 100, 'qty': 3}, {'price': 80, 'qty': 4}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'price': 120, 'qty': 6}, {'price': 90, 'qty': 8}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: items has element with price > 100 AND items has element with qty > 5
        String query = "{'items': {'$elemMatch': {'price': {'$gt': 100}}}, 'items': {'$elemMatch': {'qty': {'$gt': 5}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: price 150 > 100 ✓, qty 5 not > 5
            // Order2: price 200 > 100 ✓, qty 10 > 5 ✓
            // Order3: price 100 not > 100
            // Order4: price 120 > 100 ✓, qty 6 and 8 > 5 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order2", "Order4"), extractNamesFromResults(results));
        }
    }

    // Medium Priority: Deeply nested fields with indexes

    @Test
    void shouldUseIndexWithDeeplyNestedFieldInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-deep-nested-indexed";

        // Create index on deeply nested field (multiKey for arrays)
        IndexDefinition valueIndex = IndexDefinition.create("items-value-index", "items.details.info.value", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valueIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record1', 'items': [{'details': {'info': {'value': 100}}}, {'details': {'info': {'value': 50}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record2', 'items': [{'details': {'info': {'value': 200}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record3', 'items': [{'details': {'info': {'value': 30}}}, {'details': {'info': {'value': 40}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Record4', 'items': [{'details': {'info': {'value': 150}}}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find records with at least one item where details.info.value > 120
        String query = "{'items': {'$elemMatch': {'details.info.value': {'$gt': 120}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Record1: max value 100 < 120
            // Record2: value 200 > 120 ✓
            // Record3: max value 40 < 120
            // Record4: value 150 > 120 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Record2", "Record4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineMultipleDeeplyNestedFieldsWithIndexInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-multi-deep-indexed";

        // Create index on one of the deeply nested fields (multiKey for arrays)
        IndexDefinition countIndex = IndexDefinition.create("entries-count-index", "entries.meta.stats.count", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, countIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data1', 'entries': [{'meta': {'stats': {'count': 50, 'total': 500}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data2', 'entries': [{'meta': {'stats': {'count': 100, 'total': 800}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data3', 'entries': [{'meta': {'stats': {'count': 80, 'total': 1200}}}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Data4', 'entries': [{'meta': {'stats': {'count': 120, 'total': 1500}}}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find data with entry where meta.stats.count > 70 AND meta.stats.total > 1000
        // count condition uses index, total is filtered
        String query = "{'entries': {'$elemMatch': {'meta.stats.count': {'$gt': 70}, 'meta.stats.total': {'$gt': 1000}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Data1: count 50 < 70
            // Data2: count 100 > 70 ✓, total 800 < 1000
            // Data3: count 80 > 70 ✓, total 1200 > 1000 ✓
            // Data4: count 120 > 70 ✓, total 1500 > 1000 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Data3", "Data4"), extractNamesFromResults(results));
        }
    }

    // Medium Priority: Compound range with indexes

    @Test
    void shouldUseIndexWithCompoundRangeInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-compound-range-indexed";

        // Create index on price field (multiKey for arrays)
        IndexDefinition priceIndex = IndexDefinition.create("variants-price-index", "variants.price", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'variants': [{'price': 50, 'stock': 100}, {'price': 150, 'stock': 20}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'variants': [{'price': 80, 'stock': 50}, {'price': 120, 'stock': 30}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'variants': [{'price': 200, 'stock': 10}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'variants': [{'price': 90, 'stock': 75}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find products with variant where price is between 60 and 130 AND stock > 40
        // price condition uses index
        String query = "{'variants': {'$elemMatch': {'price': {'$gte': 60, '$lte': 130}, 'stock': {'$gt': 40}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Product1: price 50 not in [60,130] for first, price 150 not in range for second
            // Product2: price 80 in [60,130] ✓, stock 50 > 40 ✓
            // Product3: price 200 not in [60,130]
            // Product4: price 90 in [60,130] ✓, stock 75 > 40 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Product2", "Product4"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseIndexWithCompoundRangeOnScalarArrayInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-scalar-compound-range-indexed";

        // Create index on readings array (multiKey for arrays)
        IndexDefinition readingsIndex = IndexDefinition.create("readings-index", "readings", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, readingsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Sensor1', 'readings': [15, 25, 35]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Sensor2', 'readings': [45, 55, 65]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Sensor3', 'readings': [20, 40, 60]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Sensor4', 'readings': [5, 10, 15]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find sensors with at least one reading between 30 and 50 (inclusive)
        String query = "{'readings': {'$elemMatch': {'$gte': 30, '$lte': 50}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Should use RangeScanNode for indexed range query
        assertInstanceOf(RangeScanNode.class, plan, "Root should be RangeScanNode for indexed range query");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Sensor1: 35 in [30,50] ✓
            // Sensor2: 45 in [30,50] ✓
            // Sensor3: 40 in [30,50] ✓
            // Sensor4: max 15 < 30
            assertEquals(3, results.size(), "Should return exactly 3 documents");
            assertEquals(Set.of("Sensor1", "Sensor2", "Sensor3"), extractNamesFromResults(results));
        }
    }

    // Medium Priority: $ne with indexes

    @Test
    void shouldUseIndexWithNeInsideElemMatchOnDocumentArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-ne-doc-indexed";

        // Create index on items.status (multiKey for arrays)
        IndexDefinition statusIndex = IndexDefinition.create("items-status-index", "items.status", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'status': 'pending'}, {'status': 'shipped'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'status': 'cancelled'}, {'status': 'cancelled'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'status': 'delivered'}, {'status': 'cancelled'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'status': 'cancelled'}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find orders with at least one item with status != 'cancelled'
        String query = "{'items': {'$elemMatch': {'status': {'$ne': 'cancelled'}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // $ne can use index scan
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode for $ne query");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Order1: pending and shipped != cancelled ✓
            // Order2: all cancelled
            // Order3: delivered != cancelled ✓
            // Order4: only cancelled
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Order1", "Order3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseIndexWithNeInsideElemMatchOnScalarArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-ne-scalar-indexed";

        // Create index on values array (multiKey for arrays)
        IndexDefinition valuesIndex = IndexDefinition.create("values-index", "values", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valuesIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config1', 'values': [0, 0, 0]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config2', 'values': [0, 1, 0]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config3', 'values': [1, 2, 3]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Config4', 'values': [0, 0]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find configs with at least one value != 0
        String query = "{'values': {'$elemMatch': {'$ne': 0}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // $ne can use index scan
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode for $ne query");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Config1: all zeros
            // Config2: 1 != 0 ✓
            // Config3: all != 0 ✓
            // Config4: all zeros
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Config2", "Config3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldCombineNeWithIndexedConditionInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-ne-combined-indexed";

        // Create index on user field (multiKey for arrays)
        IndexDefinition userIndex = IndexDefinition.create("assignments-user-index", "assignments.user", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, userIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task1', 'assignments': [{'user': 'alice', 'role': 'owner'}, {'user': 'bob', 'role': 'viewer'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task2', 'assignments': [{'user': 'charlie', 'role': 'editor'}, {'user': 'dave', 'role': 'owner'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task3', 'assignments': [{'user': 'eve', 'role': 'viewer'}, {'user': 'frank', 'role': 'viewer'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Task4', 'assignments': [{'user': 'alice', 'role': 'editor'}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find tasks with at least one assignment where user = 'alice' AND role != 'viewer'
        // user condition uses index
        String query = "{'assignments': {'$elemMatch': {'user': 'alice', 'role': {'$ne': 'viewer'}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Should use index for user='alice'
        assertInstanceOf(IndexScanNode.class, plan, "Root should be IndexScanNode for indexed condition");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Task1: alice with owner role != viewer ✓
            // Task2: no alice
            // Task3: no alice
            // Task4: alice with editor role != viewer ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Task1", "Task4"), extractNamesFromResults(results));
        }
    }

    // Medium Priority: Large arrays with indexes

    @Test
    void shouldHandleLargeArraysWithIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-large-array-indexed";

        // Create index on values array (multiKey for arrays)
        IndexDefinition valuesIndex = IndexDefinition.create("values-index", "values", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valuesIndex);

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

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: Find documents with at least one value > 90
        String query = "{'values': {'$elemMatch': {'$gt': 90}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Data1: max is 49 < 90
            // Data2: has values 91-99 > 90 ✓
            // Data3: max is 98 > 90 ✓
            assertEquals(2, results.size(), "Should return exactly 2 documents");
            assertEquals(Set.of("Data2", "Data3"), extractNamesFromResults(results));
        }
    }

}
