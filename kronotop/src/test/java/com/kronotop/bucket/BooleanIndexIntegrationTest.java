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

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.kronotop.TestUtil;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.pipeline.BasePipelineTest;
import com.kronotop.bucket.pipeline.IndexScanNode;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.bucket.pipeline.QueryOptions;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class BooleanIndexIntegrationTest extends BasePipelineTest {

    @Test
    void shouldQueryBooleanIndexWithTrue() {
        SingleFieldIndexDefinition activeIndex = SingleFieldIndexDefinition.create("active-index", "active", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'active': true}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        assertInstanceOf(IndexScanNode.class, planWithParams.plan());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"John\", \"active\": true}",
                "{\"name\": \"Bob\", \"active\": true}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldQueryBooleanIndexWithFalse() {
        SingleFieldIndexDefinition activeIndex = SingleFieldIndexDefinition.create("active-index", "active", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'active': false}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        assertInstanceOf(IndexScanNode.class, planWithParams.plan());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"Alice\", \"active\": false}",
                "{\"name\": \"Claire\", \"active\": false}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldQueryBooleanIndexWithGtFalse() {
        SingleFieldIndexDefinition activeIndex = SingleFieldIndexDefinition.create("active-index", "active", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // $gt: false is equivalent to true because true > false
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'active': {'$gt': false}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"John\", \"active\": true}",
                "{\"name\": \"Bob\", \"active\": true}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldQueryBooleanIndexWithLtFalse() {
        SingleFieldIndexDefinition activeIndex = SingleFieldIndexDefinition.create("active-index", "active", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // $lt: false returns empty because nothing is less than false
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'active': {'$lt': false}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(0, results.size());
        }
    }

    @Test
    void shouldQueryBooleanIndexWithLtTrue() {
        SingleFieldIndexDefinition activeIndex = SingleFieldIndexDefinition.create("active-index", "active", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // $lt: true is equivalent to false because false < true
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'active': {'$lt': true}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"Alice\", \"active\": false}",
                "{\"name\": \"Claire\", \"active\": false}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldReturnEmptyWhenComparingBooleanWithString() {
        SingleFieldIndexDefinition activeIndex = SingleFieldIndexDefinition.create("active-index", "active", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Comparing boolean field with string value returns empty (type mismatch)
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'active': {'$lt': 'true'}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(0, results.size());
        }
    }
}
