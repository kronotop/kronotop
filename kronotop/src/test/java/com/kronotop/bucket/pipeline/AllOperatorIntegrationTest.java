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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AllOperatorIntegrationTest extends BasePipelineTest {
    @Test
    void shouldMatchArrayContainingAllValues() {
        final String TEST_BUCKET_NAME = "test-bucket-all-operator";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python', 'go'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'rust'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['python', 'go'], 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $all: match documents where tags contains ALL of ['java', 'python']
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$all': ['java', 'python']}}");
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

        // Only Alice has both 'java' AND 'python'
        List<String> expectedResult = List.of(
                "{\"tags\": [\"java\", \"python\", \"go\"], \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldNotMatchWhenNotAllValuesPresent() {
        final String TEST_BUCKET_NAME = "test-bucket-all-not-match";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'rust'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['python', 'go'], 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Neither document has both 'java' AND 'python'
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$all': ['java', 'python']}}");
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
    void shouldMatchSingleValueAll() {
        final String TEST_BUCKET_NAME = "test-bucket-all-single";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'go'], 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Single value $all - equivalent to checking if array contains the value
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$all': ['java']}}");
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
                "{\"tags\": [\"java\", \"python\"], \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchAllWithNumericTypes() {
        final String TEST_BUCKET_NAME = "test-bucket-all-numeric";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'scores': [10, 20, 30], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'scores': [10, 40, 50], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'scores': [20, 30, 40], 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Match documents with both 10 AND 20 in scores
        PipelineNode plan = createExecutionPlan(metadata, "{'scores': {'$all': [10, 20]}}");
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

        // Only Alice has both 10 AND 20
        List<String> expectedResult = List.of(
                "{\"scores\": [10, 20, 30], \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchNestedArrayAsElement() {
        // $all can match nested arrays as elements
        final String TEST_BUCKET_NAME = "test-bucket-all-nested-array";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'matrix': [['A', 'B'], ['C', 'D']], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'matrix': [['A', 'B'], ['E', 'F']], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'matrix': [['X', 'Y']], 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Match documents where matrix contains ['A', 'B'] as an element
        PipelineNode plan = createExecutionPlan(metadata, "{'matrix': {'$all': [['A', 'B']]}}");
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

        // Alice and Bob both have ['A', 'B'] as an element
        List<String> expectedResult = List.of(
                "{\"matrix\": [[\"A\", \"B\"], [\"C\", \"D\"]], \"name\": \"Alice\"}",
                "{\"matrix\": [[\"A\", \"B\"], [\"E\", \"F\"]], \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleAllWithAndOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-all-with-and";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'rust'], 'status': 'active', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Match documents with both 'java' AND 'python' in tags AND status = 'active'
        PipelineNode plan = createExecutionPlan(metadata,
                "{'$and': [{'tags': {'$all': ['java', 'python']}}, {'status': 'active'}]}");
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

        // Only Alice has both tags AND active status
        List<String> expectedResult = List.of(
                "{\"tags\": [\"java\", \"python\"], \"status\": \"active\", \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleAllOnNestedField() {
        final String TEST_BUCKET_NAME = "test-bucket-all-nested-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'user': {'skills': ['java', 'python', 'go']}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'skills': ['java', 'rust']}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'skills': ['python']}, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'user.skills': {'$all': ['java', 'python']}}");
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

        // Only Alice has both 'java' AND 'python' in user.skills
        List<String> expectedResult = List.of(
                "{\"user\": {\"skills\": [\"java\", \"python\", \"go\"]}, \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldNotMatchNonArrayField() {
        final String TEST_BUCKET_NAME = "test-bucket-all-non-array";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tag': 'java', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tag': 'python', 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $all on a non-array field should not match
        PipelineNode plan = createExecutionPlan(metadata, "{'tag': {'$all': ['java']}}");
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
    void shouldHandleMissingField() {
        final String TEST_BUCKET_NAME = "test-bucket-all-missing-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // $all on missing field should not match
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$all': ['java']}}");
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

        // Only Alice has the 'tags' field
        List<String> expectedResult = List.of(
                "{\"tags\": [\"java\", \"python\"], \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchWithDuplicateValuesInQuery() {
        final String TEST_BUCKET_NAME = "test-bucket-all-duplicates";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'go'], 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Duplicate 'java' in $all should still work
        PipelineNode plan = createExecutionPlan(metadata, "{'tags': {'$all': ['java', 'java']}}");
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
                "{\"tags\": [\"java\", \"python\"], \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }
}
