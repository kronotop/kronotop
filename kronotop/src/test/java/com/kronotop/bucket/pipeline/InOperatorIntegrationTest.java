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
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class InOperatorIntegrationTest extends BasePipelineTest {
    @Test
    void shouldMatchDocumentsWithInOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-in-operator";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['admin', 'editor']}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"name\": \"Alice\"}",
                "{\"role\": \"editor\", \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchDocumentsWithInOperatorViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-in-operator-indexscan";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $in is transformed to $or, resulting in UnionNode with multiple IndexScanNodes
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['admin', 'editor']}}");
        assertInstanceOf(UnionNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // UnionNode doesn't guarantee order, so sort before comparison
        Collections.sort(actualResult);
        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"name\": \"Alice\"}",
                "{\"role\": \"editor\", \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchDocumentsWithMixedTypesInOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-in-operator-mixed-types";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 20, 'name': 'Eve'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['admin', 'editor', 20]}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"name\": \"Alice\"}",
                "{\"role\": \"editor\", \"name\": \"Charlie\"}",
                "{\"role\": 20, \"name\": \"Eve\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchSingleValueIn() {
        final String TEST_BUCKET_NAME = "test-bucket-single-value-in";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Single value $in is optimized to EQ
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['admin']}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"role\": \"admin\", \"name\": \"Alice\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchSingleValueInViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-single-value-in-indexed";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Single value $in is optimized to EQ, uses IndexScanNode
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['admin']}}");
        assertInstanceOf(IndexScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"role\": \"admin\", \"name\": \"Alice\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchNoDocumentsWhenNoValuesMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-no-match-in";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['superuser', 'guest']}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        assertTrue(actualResult.isEmpty());
    }

    @Test
    void shouldMatchNoDocumentsWhenNoValuesMatchViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-no-match-in-indexed";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['superuser', 'guest']}}");
        assertInstanceOf(UnionNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        assertTrue(actualResult.isEmpty());
    }

    @Test
    void shouldReturnEmptyForEmptyIn() {
        final String TEST_BUCKET_NAME = "test-bucket-empty-in";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Empty $in is transformed to LogicalFalse, resulting in null plan
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': []}}");
        assertNull(plan);
    }

    @Test
    void shouldMatchNullInList() {
        final String TEST_BUCKET_NAME = "test-bucket-null-in-list";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': null, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': [null, 'admin']}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"name\": \"Alice\"}",
                "{\"role\": null, \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleInWithAndOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-in-with-and";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'active', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"status\": \"active\", \"name\": \"Alice\"}",
                "{\"role\": \"editor\", \"status\": \"active\", \"name\": \"Diana\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleInWithAndOperatorViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-in-with-and-indexed";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'active', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $in on indexed field combined with $and results in UnionNode with residual predicate
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");
        assertInstanceOf(UnionNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        Collections.sort(actualResult);
        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"status\": \"active\", \"name\": \"Alice\"}",
                "{\"role\": \"editor\", \"status\": \"active\", \"name\": \"Diana\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleInWithOrOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-in-with-or-no-index";

        // No indexes created
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'status': 'inactive', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'inactive', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $or: role in [admin, editor] OR status = active
        // Should match: Alice (admin), Bob (active), Diana (editor)
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$or': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        Collections.sort(actualResult);
        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"status\": \"inactive\", \"name\": \"Alice\"}",
                "{\"role\": \"editor\", \"status\": \"inactive\", \"name\": \"Diana\"}",
                "{\"role\": \"user\", \"status\": \"active\", \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleInWithOrOperatorViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-in-with-or-indexed";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'status': 'inactive', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'inactive', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $or: role in [admin, editor] OR status = active
        // With index on role, this becomes UnionNode
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$or': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");
        assertInstanceOf(UnionNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        Collections.sort(actualResult);
        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"status\": \"inactive\", \"name\": \"Alice\"}",
                "{\"role\": \"editor\", \"status\": \"inactive\", \"name\": \"Diana\"}",
                "{\"role\": \"user\", \"status\": \"active\", \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleInOnNestedField() {
        final String TEST_BUCKET_NAME = "test-bucket-in-nested-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'user': {'role': 'admin'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'role': 'user'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'role': 'editor'}, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'user.role': {'$in': ['admin', 'editor']}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of(
                "{\"user\": {\"role\": \"admin\"}, \"name\": \"Alice\"}",
                "{\"user\": {\"role\": \"editor\"}, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleInWithNumericTypes() {
        final String TEST_BUCKET_NAME = "test-bucket-in-numeric";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$in': [25, 35, 50]}}");
        assertInstanceOf(UnionNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        Collections.sort(actualResult);
        List<String> expectedResult = List.of(
                "{\"age\": 25, \"name\": \"Alice\"}",
                "{\"age\": 35, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleDuplicateValuesInList() {
        final String TEST_BUCKET_NAME = "test-bucket-duplicate-values-in";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Duplicate 'admin' in the list should still return only one admin document
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['admin', 'admin', 'editor']}}");
        assertInstanceOf(UnionNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        Collections.sort(actualResult);
        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"name\": \"Alice\"}",
                "{\"role\": \"editor\", \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchArrayFieldWithInOperator() {
        // If the field holds an array, then the $in operator selects the documents whose
        // field holds an array that contains at least one element that matches a value
        // in the specified array.
        final String TEST_BUCKET_NAME = "test-bucket-array-field-in";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python', 'go'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'c++'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['javascript', 'typescript', 'python'], 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['ruby', 'elixir'], 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for documents where tags array contains 'python' or 'rust'
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$in': ['python', 'rust']}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match Alice (has 'python'), Bob (has 'rust'), and Charlie (has 'python')
        List<String> expectedResult = List.of(
                "{\"tags\": [\"java\", \"python\", \"go\"], \"name\": \"Alice\"}",
                "{\"tags\": [\"rust\", \"c++\"], \"name\": \"Bob\"}",
                "{\"tags\": [\"javascript\", \"typescript\", \"python\"], \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchMixedTypeDocumentsWithMixedTypeInQuery() {
        final String TEST_BUCKET_NAME = "test-bucket-in-mixed-types-query";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 3, 'name': 'Eve'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 5, 'name': 'Frank'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Mixed types in $in list causes type mismatch -> falls back to FullScan (primary index)
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['admin', 'editor', 3]}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        Collections.sort(actualResult);

        // Should match Alice (admin), Charlie (editor), and Eve (3)
        // NOT Bob (user), Diana (guest), or Frank (5)
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"role\": \"admin\", \"name\": \"Alice\"}",
                "{\"role\": \"editor\", \"name\": \"Charlie\"}",
                "{\"role\": 3, \"name\": \"Eve\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldNotMatchDocumentsWithMissingField() {
        final String TEST_BUCKET_NAME = "test-bucket-in-missing-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $in on field that doesn't exist in some documents
        // Documents without the field should NOT match (field value is not in the list)
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$in': ['admin', 'user']}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match Alice (admin) and Bob (user)
        // NOT Charlie (missing field) or Diana (guest)
        List<String> expectedResult = List.of(
                "{\"role\": \"admin\", \"name\": \"Alice\"}",
                "{\"role\": \"user\", \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchNestedArrayAsTopLevelElement() {
        // $in performs shallow comparison - nested arrays are matched as values, not searched recursively
        final String TEST_BUCKET_NAME = "test-bucket-in-nested-array";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['A', 'B'], 'C'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['X', 'Y'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['A', 'B'], 'Z'], 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for documents where tags contains the nested array ['A', 'B'] as a top-level element
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$in': [['A', 'B']]}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match Alice and Charlie (both have ['A', 'B'] as a top-level element)
        // NOT Bob (doesn't have ['A', 'B'])
        List<String> expectedResult = List.of(
                "{\"tags\": [[\"A\", \"B\"], \"C\"], \"name\": \"Alice\"}",
                "{\"tags\": [[\"A\", \"B\"], \"Z\"], \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldNotMatchScalarInsideNestedArray() {
        // $in does NOT recursively search inside nested arrays
        final String TEST_BUCKET_NAME = "test-bucket-in-no-recursive";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['A', 'B'], 'C'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['A', 'B'], 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for 'A' - should only match Bob where 'A' is a top-level element
        // Alice has 'A' inside nested array ['A', 'B'], not at top level
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$in': ['A']}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match only Bob ('A' is top-level), NOT Alice ('A' is inside nested array)
        List<String> expectedResult = List.of(
                "{\"tags\": [\"A\", \"B\"], \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchTopLevelScalarInMixedArray() {
        // $in matches top-level scalars even when array contains nested arrays
        final String TEST_BUCKET_NAME = "test-bucket-in-mixed-nested";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['A', 'B'], 'C'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['X', 'Y'], 'Z'], 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for 'C' - should match Alice where 'C' is a top-level element
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$in': ['C']}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match Alice ('C' is top-level), NOT Bob
        List<String> expectedResult = List.of(
                "{\"tags\": [[\"A\", \"B\"], \"C\"], \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchArrayFieldWithInOperatorViaMultiKeyIndex() {
        // $in on array field with multi-key index uses index scan
        // Unlike $nin, $in semantics work correctly with multi-key indexes:
        // EQ on multi-key matches documents where AT LEAST ONE element matches,
        // which is exactly what $in requires.
        final String TEST_BUCKET_NAME = "test-bucket-in-multikey-index";

        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python', 'go'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'c++'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['javascript', 'typescript'], 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['ruby', 'elixir'], 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for documents where tags array contains 'python' or 'rust'
        // Multi-key index: uses UnionNode with IndexScanNodes (EQ works correctly for arrays)
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$in': ['python', 'rust']}}");
        assertInstanceOf(UnionNode.class, plan);

        // Verify UnionNode uses the created index for both conditions
        UnionNode unionNode = (UnionNode) plan;
        assertEquals(2, unionNode.children().size());
        for (PipelineNode child : unionNode.children()) {
            assertInstanceOf(IndexScanNode.class, child);
            IndexScanNode indexScanNode = (IndexScanNode) child;
            assertEquals(tagsIndex.id(), indexScanNode.getIndexDefinition().id());
        }

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        Collections.sort(actualResult);

        // Should match Alice (has 'python') and Bob (has 'rust')
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"tags\": [\"java\", \"python\", \"go\"], \"name\": \"Alice\"}",
                "{\"tags\": [\"rust\", \"c++\"], \"name\": \"Bob\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldOptimizeSingleValueInOnMultiKeyIndexToIndexScan() {
        // Single value $in on multiKey index optimizes to IndexScanNode (not UnionNode)
        final String TEST_BUCKET_NAME = "test-bucket-in-multikey-single";

        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'go'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['python', 'javascript'], 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Single value $in optimizes to EQ → single IndexScanNode
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$in': ['python']}}");
        assertInstanceOf(IndexScanNode.class, plan);

        // Verify it uses the correct index
        IndexScanNode indexScanNode = (IndexScanNode) plan;
        assertEquals(tagsIndex.id(), indexScanNode.getIndexDefinition().id());

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        Collections.sort(actualResult);

        // Should match Alice and Charlie (both have 'python')
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"tags\": [\"java\", \"python\"], \"name\": \"Alice\"}",
                "{\"tags\": [\"python\", \"javascript\"], \"name\": \"Charlie\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchIntArrayFieldWithInOperatorViaMultiKeyIndex() {
        // INT32 multiKey index basic happy path
        final String TEST_BUCKET_NAME = "test-bucket-in-multikey-int";

        IndexDefinition scoresIndex = IndexDefinition.create("scores-index", "scores", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoresIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'scores': [85, 90, 95], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'scores': [70, 75, 80], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'scores': [90, 92], 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'scores': [60, 65], 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for documents with score 90 or 70
        PipelineNode plan = createExecutionPlan(metadata, "{'scores': {'$in': [90, 70]}}");
        assertInstanceOf(UnionNode.class, plan);

        // Verify UnionNode uses the created index for both conditions
        UnionNode unionNode = (UnionNode) plan;
        assertEquals(2, unionNode.children().size());
        for (PipelineNode child : unionNode.children()) {
            assertInstanceOf(IndexScanNode.class, child);
            IndexScanNode indexScanNode = (IndexScanNode) child;
            assertEquals(scoresIndex.id(), indexScanNode.getIndexDefinition().id());
        }

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        Collections.sort(actualResult);

        // Should match Alice (has 90), Bob (has 70), Charlie (has 90)
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"scores\": [85, 90, 95], \"name\": \"Alice\"}",
                "{\"scores\": [70, 75, 80], \"name\": \"Bob\"}",
                "{\"scores\": [90, 92], \"name\": \"Charlie\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldCombineInOnMultiKeyIndexWithOtherConditions() {
        // $in on multiKey index combined with $and and other predicates
        final String TEST_BUCKET_NAME = "test-bucket-in-multikey-combined";

        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'go'], 'status': 'active', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['python', 'javascript'], 'status': 'inactive', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['ruby'], 'status': 'active', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Combined query: tags contains 'python' or 'rust' AND status is 'active'
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$and': [{'tags': {'$in': ['python', 'rust']}}, {'status': 'active'}]}");
        assertInstanceOf(UnionNode.class, plan);

        // Verify UnionNode uses the created index for both $in values
        UnionNode unionNode = (UnionNode) plan;
        assertEquals(2, unionNode.children().size());
        for (PipelineNode child : unionNode.children()) {
            // Child may be IndexScanNode directly or have FilterNode attached
            PipelineNode scanNode = child;
            while (scanNode != null && !(scanNode instanceof IndexScanNode)) {
                scanNode = scanNode.next();
            }
            assertNotNull(scanNode, "IndexScanNode should be present in child chain");
            assertInstanceOf(IndexScanNode.class, scanNode);
            IndexScanNode indexScanNode = (IndexScanNode) scanNode;
            assertEquals(tagsIndex.id(), indexScanNode.getIndexDefinition().id());
        }

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        Collections.sort(actualResult);

        // Should match Alice (has 'python', status 'active') and Bob (has 'rust', status 'active')
        // NOT Charlie (has 'python' but status 'inactive')
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"tags\": [\"java\", \"python\"], \"status\": \"active\", \"name\": \"Alice\"}",
                "{\"tags\": [\"rust\", \"go\"], \"status\": \"active\", \"name\": \"Bob\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }
}
