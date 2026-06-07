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
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SizeOperatorIntegrationTest extends BasePipelineTest {

    @Test
    void shouldMatchArrayWithExactSize() {
        final String TEST_BUCKET_NAME = "test-bucket-size-exact";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python', 'go'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'rust'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['python'], 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // $size: match documents where tags array has exactly 2 elements
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'tags': {'$size': 2}}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // Only Bob has exactly 2 tags
        List<String> expectedResult = List.of(
                "{\"tags\": [\"java\", \"rust\"], \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchEmptyArray() {
        final String TEST_BUCKET_NAME = "test-bucket-size-empty";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': [], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java'], 'name': 'Bob'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // $size: match documents where tags array is empty
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'tags': {'$size': 0}}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"tags\": [], \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldNotMatchWhenSizeDiffers() {
        final String TEST_BUCKET_NAME = "test-bucket-size-no-match";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust'], 'name': 'Bob'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // No document has exactly 3 tags
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'tags': {'$size': 3}}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertTrue(actualResult.isEmpty());
    }

    @Test
    void shouldNotMatchNonArrayField() {
        final String TEST_BUCKET_NAME = "test-bucket-size-non-array";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // $size on a non-array field should not match
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': {'$size': 5}}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertTrue(actualResult.isEmpty());
    }

    @Test
    void shouldNotMatchMissingField() {
        final String TEST_BUCKET_NAME = "test-bucket-size-missing";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // $size on a missing field should not match
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'tags': {'$size': 0}}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertTrue(actualResult.isEmpty());
    }

    @Test
    void shouldHandleSizeWithAndOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-size-with-and";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'rust'], 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['go'], 'status': 'active', 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Match documents with exactly 2 tags AND status = 'active'
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'$and': [{'tags': {'$size': 2}}, {'status': 'active'}]}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // Only Alice has 2 tags AND is active
        List<String> expectedResult = List.of(
                "{\"tags\": [\"java\", \"python\"], \"status\": \"active\", \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleSizeWithOrOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-size-with-or";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['java', 'python'], 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['go'], 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'tags': ['rust', 'c', 'cpp'], 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Match documents with exactly 1 tag OR exactly 3 tags
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'$or': [{'tags': {'$size': 1}}, {'tags': {'$size': 3}}]}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // Bob has 1 tag, Charlie has 3 tags
        List<String> expectedResult = List.of(
                "{\"tags\": [\"go\"], \"name\": \"Bob\"}",
                "{\"tags\": [\"rust\", \"c\", \"cpp\"], \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }
}
