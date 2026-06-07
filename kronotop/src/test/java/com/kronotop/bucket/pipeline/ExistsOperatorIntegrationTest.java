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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ExistsOperatorIntegrationTest extends BasePipelineTest {

    @Test
    void shouldHandleExistsOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-exists-operator";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'George'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query for documents where 'age' field exists (not null, not missing)
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$exists': true}}");
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
                "{\"age\": 25, \"name\": \"John\"}",
                "{\"age\": null, \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchFieldWithValue() {
        final String TEST_BUCKET_NAME = "test-bucket-exists-field-with-value";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$exists': true}}");
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

        List<String> expectedResult = List.of("{\"age\": 25}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchFieldWithNullValue() {
        final String TEST_BUCKET_NAME = "test-bucket-exists-field-with-null";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$exists': true}}");
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

        List<String> expectedResult = List.of("{\"age\": null}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldNotMatchMissingField() {
        final String TEST_BUCKET_NAME = "test-bucket-exists-false";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$exists': false}}");
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

        List<String> expectedResult = List.of("{\"name\": \"John\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchAllMissingFields() {
        final String TEST_BUCKET_NAME = "test-bucket-exists-false-multiple";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$exists': false}}");
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
                "{\"name\": \"Alice\"}",
                "{\"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleNestedFieldExists() {
        final String TEST_BUCKET_NAME = "test-bucket-exists-nested";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'user': {'name': 'John'}}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {}}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'user.name': {'$exists': true}}");
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

        List<String> expectedResult = List.of("{\"user\": {\"name\": \"John\"}}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldCombineExistsWithOtherOperators() {
        final String TEST_BUCKET_NAME = "test-bucket-exists-combined";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Jane'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$exists': true}, 'name': {'$eq': 'John'}}");
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

        List<String> expectedResult = List.of("{\"age\": 25, \"name\": \"John\"}");
        assertEquals(expectedResult, actualResult);
    }
}
