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

class NinOperatorIntegrationTest extends BasePipelineTest {
    @Test
    void shouldMatchDocumentsWithNinOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-operator";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $nin: match documents where role is NOT in ['admin', 'editor']
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['admin', 'editor']}}");
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

        // Should match Bob (user) and Diana (guest), NOT Alice (admin) or Charlie (editor)
        List<String> expectedResult = List.of(
                "{\"role\": \"user\", \"name\": \"Bob\"}",
                "{\"role\": \"guest\", \"name\": \"Diana\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchDocumentsWithNinOperatorWithIndex() {
        // $nin with index: rewritten to AND(NE, NE...) -> IndexScanNode with residual predicate
        final String TEST_BUCKET_NAME = "test-bucket-nin-operator-indexed";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $nin: ['admin', 'editor'] -> AND(NE('admin'), NE('editor'))
        // -> IndexScanNode (most selective NE) with residual predicate (other NE)
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['admin', 'editor']}}");
        assertInstanceOf(IndexScanNode.class, plan);
        assertInstanceOf(TransformWithResidualPredicateNode.class, plan.next());
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

        // Should match Bob (user) and Diana (guest), NOT Alice (admin) or Charlie (editor)
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"role\": \"user\", \"name\": \"Bob\"}",
                "{\"role\": \"guest\", \"name\": \"Diana\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchMixedTypeDocumentsWithMixedTypeNinQuery() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-mixed-types-indexed";

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

        // Mixed types in $nin list causes type mismatch -> falls back to FullScan (primary index)
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['admin', 'editor', 3]}}");
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

        // Should match Bob (user), Diana (guest), and Frank (5)
        // NOT Alice (admin), Charlie (editor), or Eve (3)
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"role\": \"user\", \"name\": \"Bob\"}",
                "{\"role\": \"guest\", \"name\": \"Diana\"}",
                "{\"role\": 5, \"name\": \"Frank\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchDocumentsWithMixedTypesNinOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-operator-mixed-types";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 20, 'name': 'Eve'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Exclude 'admin', 'editor', and 20
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['admin', 'editor', 20]}}");
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

        // Should match Bob (user) and Diana (guest)
        List<String> expectedResult = List.of(
                "{\"role\": \"user\", \"name\": \"Bob\"}",
                "{\"role\": \"guest\", \"name\": \"Diana\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchSingleValueNin() {
        final String TEST_BUCKET_NAME = "test-bucket-single-value-nin";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Single value $nin excludes only 'admin'
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['admin']}}");
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
                "{\"role\": \"user\", \"name\": \"Bob\"}",
                "{\"role\": \"editor\", \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchSingleValueNinViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-single-value-nin-indexed";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Single value $nin is optimized to NE, uses IndexScanNode
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['admin']}}");
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
        Collections.sort(actualResult);

        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"role\": \"user\", \"name\": \"Bob\"}",
                "{\"role\": \"editor\", \"name\": \"Charlie\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchAllDocumentsWhenValuesNotPresent() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-no-match";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Exclude values that don't exist - all documents should match
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['superuser', 'guest']}}");
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
                "{\"role\": \"user\", \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchAllDocumentsWhenValuesNotPresentViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-no-match-indexed";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Exclude values that don't exist - all documents should match
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['superuser', 'guest']}}");
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
        Collections.sort(actualResult);

        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"role\": \"admin\", \"name\": \"Alice\"}",
                "{\"role\": \"user\", \"name\": \"Bob\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldReturnAllForEmptyNin() {
        final String TEST_BUCKET_NAME = "test-bucket-empty-nin";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Empty $nin matches everything (PhysicalTrue)
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': []}}");
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
                "{\"role\": \"user\", \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldExcludeNullInNinList() {
        final String TEST_BUCKET_NAME = "test-bucket-null-nin-list";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': null, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Exclude null and 'admin'
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': [null, 'admin']}}");
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

        // Only Charlie (user) should match
        List<String> expectedResult = List.of(
                "{\"role\": \"user\", \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleNinWithAndOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-with-and";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'active', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // role NOT IN ['admin', 'editor'] AND status = 'active'
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$and': [{'role': {'$nin': ['admin', 'editor']}}, {'status': 'active'}]}");
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

        // Only Charlie matches (user AND active)
        List<String> expectedResult = List.of(
                "{\"role\": \"user\", \"status\": \"active\", \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleNinWithAndOperatorViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-with-and-indexed";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'active', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $nin on indexed field combined with $and
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$and': [{'role': {'$nin': ['admin', 'editor']}}, {'status': 'active'}]}");
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
        Collections.sort(actualResult);

        // Only Charlie matches (user AND active)
        List<String> expectedResult = List.of(
                "{\"role\": \"user\", \"status\": \"active\", \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleNinWithOrOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-with-or";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'status': 'inactive', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'inactive', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // role NOT IN ['admin', 'editor'] OR status = 'active'
        // Should match: Bob (active), Charlie (guest, not admin/editor)
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$or': [{'role': {'$nin': ['admin', 'editor']}}, {'status': 'active'}]}");
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

        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"role\": \"user\", \"status\": \"active\", \"name\": \"Bob\"}",
                "{\"role\": \"guest\", \"status\": \"inactive\", \"name\": \"Charlie\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleNinWithOrOperatorViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-with-or-indexed";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'status': 'inactive', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'inactive', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $or: role not in [admin, editor] OR status = active
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$or': [{'role': {'$nin': ['admin', 'editor']}}, {'status': 'active'}]}");
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

        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"role\": \"user\", \"status\": \"active\", \"name\": \"Bob\"}",
                "{\"role\": \"guest\", \"status\": \"inactive\", \"name\": \"Charlie\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleNinOnNestedField() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-nested-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'user': {'role': 'admin'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'role': 'user'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'role': 'editor'}, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'user.role': {'$nin': ['admin', 'editor']}}");
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

        // Only Bob (user) should match
        List<String> expectedResult = List.of(
                "{\"user\": {\"role\": \"user\"}, \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleNinWithNumericTypes() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-numeric";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Exclude ages 25 and 35
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$nin': [25, 35]}}");
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
        Collections.sort(actualResult);

        // Bob (30) and Diana (40) should match
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"age\": 30, \"name\": \"Bob\"}",
                "{\"age\": 40, \"name\": \"Diana\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleDuplicateValuesInNinList() {
        final String TEST_BUCKET_NAME = "test-bucket-duplicate-values-nin";

        IndexDefinition roleIndex = IndexDefinition.create("role-index", "role", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Duplicate 'admin' in the list should still exclude admin correctly
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['admin', 'admin', 'editor']}}");
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
        Collections.sort(actualResult);

        // Only Bob (user) should match
        List<String> expectedResult = List.of(
                "{\"role\": \"user\", \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchArrayFieldWithNinOperator() {
        // If the field holds an array, then the $nin operator selects the documents whose
        // field holds an array that does NOT contain any element that matches a value
        // in the specified array.
        final String TEST_BUCKET_NAME = "test-bucket-array-field-nin";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python', 'go'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'c++'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['javascript', 'typescript', 'python'], 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['ruby', 'elixir'], 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for documents where tags array does NOT contain 'python' or 'rust'
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$nin': ['python', 'rust']}}");
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

        // Should match only Diana (no 'python' or 'rust' in tags)
        // NOT Alice (has 'python'), Bob (has 'rust'), or Charlie (has 'python')
        List<String> expectedResult = List.of(
                "{\"tags\": [\"ruby\", \"elixir\"], \"name\": \"Diana\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchDocumentsWithMissingField() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-missing-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'guest', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $nin on field that doesn't exist in some documents
        // Documents without the field should match (field value is not in exclusion list)
        PipelineNode plan = createExecutionPlan(metadata, "{'role': {'$nin': ['admin', 'user']}}");
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

        // Should match Charlie (missing field) and Diana (guest)
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"name\": \"Charlie\"}",
                "{\"role\": \"guest\", \"name\": \"Diana\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldExcludeNestedArrayAsTopLevelElement() {
        // $nin performs shallow comparison - nested arrays are matched as values, not searched recursively
        final String TEST_BUCKET_NAME = "test-bucket-nin-nested-array";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['A', 'B'], 'C'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['X', 'Y'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['A', 'B'], 'Z'], 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query to exclude documents where tags contains the nested array ['A', 'B'] as a top-level element
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$nin': [['A', 'B']]}}");
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

        // Should match only Bob (doesn't have ['A', 'B'] as a top-level element)
        // NOT Alice or Charlie (both have ['A', 'B'])
        List<String> expectedResult = List.of(
                "{\"tags\": [\"X\", \"Y\"], \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldNotExcludeScalarInsideNestedArray() {
        // $nin does NOT recursively search inside nested arrays
        final String TEST_BUCKET_NAME = "test-bucket-nin-no-recursive";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['A', 'B'], 'C'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['A', 'B'], 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query to exclude 'A' - should exclude Bob where 'A' is a top-level element
        // Alice has 'A' inside nested array ['A', 'B'], not at top level, so Alice should match
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$nin': ['A']}}");
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

        // Should match only Alice ('A' is NOT top-level, it's inside nested array)
        // NOT Bob ('A' is top-level, so excluded)
        List<String> expectedResult = List.of(
                "{\"tags\": [[\"A\", \"B\"], \"C\"], \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldExcludeTopLevelScalarInMixedArray() {
        // $nin excludes documents with top-level scalars even when array contains nested arrays
        final String TEST_BUCKET_NAME = "test-bucket-nin-mixed-nested";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['A', 'B'], 'C'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': [['X', 'Y'], 'Z'], 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query to exclude 'C' - should exclude Alice where 'C' is a top-level element
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$nin': ['C']}}");
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

        // Should match only Bob ('C' is not present), NOT Alice ('C' is top-level)
        List<String> expectedResult = List.of(
                "{\"tags\": [[\"X\", \"Y\"], \"Z\"], \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    // Multi-key index tests (indexes on array fields)
    // NOTE: $nin on multi-key indexes falls back to FullScan because NE on multi-key indexes
    // has incorrect semantics (matches documents where AT LEAST ONE element doesn't match,
    // but $nin requires documents where NO element matches).

    @Test
    void shouldFallbackToFullScanForNinOnMultiKeyIndex() {
        // $nin on array field with multi-key index falls back to FullScan
        final String TEST_BUCKET_NAME = "test-bucket-array-nin-multikey";

        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python', 'go'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'c++'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['javascript', 'typescript', 'python'], 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['ruby', 'elixir'], 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for documents where tags array does NOT contain 'python' or 'rust'
        // Multi-key index: falls back to FullScan for correct array semantics
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$nin': ['python', 'rust']}}");
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

        // Should match only Diana (no 'python' or 'rust' in tags)
        // NOT Alice (has 'python'), Bob (has 'rust'), or Charlie (has 'python')
        List<String> expectedResult = List.of(
                "{\"tags\": [\"ruby\", \"elixir\"], \"name\": \"Diana\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFallbackToFullScanForSingleValueNinOnMultiKeyIndex() {
        // Single value $nin on multi-key index also falls back to FullScan
        final String TEST_BUCKET_NAME = "test-bucket-array-nin-single-multikey";

        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'go'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['python', 'javascript'], 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['ruby'], 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Single value $nin on multi-key index falls back to FullScan
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$nin': ['python']}}");
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

        // Should match Bob and Diana (no 'python' in tags)
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"tags\": [\"rust\", \"go\"], \"name\": \"Bob\"}",
                "{\"tags\": [\"ruby\"], \"name\": \"Diana\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFallbackToFullScanForNinOnIntMultiKeyIndex() {
        // $nin on integer array field with multi-key index falls back to FullScan
        final String TEST_BUCKET_NAME = "test-bucket-int-array-nin-multikey";

        IndexDefinition scoresIndex = IndexDefinition.create("scores-index", "scores", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoresIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'scores': [85, 90, 95], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'scores': [70, 75], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'scores': [90, 80], 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'scores': [60, 65, 70], 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Exclude documents with score 90 or 70
        // Multi-key index: falls back to FullScan for correct array semantics
        PipelineNode plan = createExecutionPlan(metadata, "{'scores': {'$nin': [90, 70]}}");
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

        // Alice has 90, Bob has 70, Charlie has 90, Diana has 70
        // So NO documents match because all have either 90 or 70
        assertTrue(actualResult.isEmpty(), "No documents should match as all contain 90 or 70");
    }

    @Test
    void shouldCombineNinOnMultiKeyIndexWithOtherConditions() {
        // $nin on multi-key index combined with other conditions falls back to FullScan
        final String TEST_BUCKET_NAME = "test-bucket-array-nin-combined-multikey";

        IndexDefinition tagsIndex = IndexDefinition.create("tags-index", "tags", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'go'], 'status': 'active', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['python', 'javascript'], 'status': 'inactive', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['ruby'], 'status': 'active', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $nin on multi-key index combined with status filter - falls back to FullScan
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$and': [{'tags': {'$nin': ['python']}}, {'status': 'active'}]}");
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

        // Should match Bob and Diana (no 'python' AND status='active')
        // NOT Alice (has 'python'), NOT Charlie (inactive)
        List<String> expectedResult = new ArrayList<>(List.of(
                "{\"tags\": [\"rust\", \"go\"], \"status\": \"active\", \"name\": \"Bob\"}",
                "{\"tags\": [\"ruby\"], \"status\": \"active\", \"name\": \"Diana\"}"
        ));
        Collections.sort(expectedResult);
        assertEquals(expectedResult, actualResult);
    }
}
