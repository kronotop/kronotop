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
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.pipeline.*;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class BooleanIndexIntegrationTest extends BasePipelineTest {

    @Test
    void shouldQueryBooleanIndexWithTrue() {
        IndexDefinition activeIndex = IndexDefinition.create("active-index", "active", BsonType.BOOLEAN);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'active': true}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        assertInstanceOf(IndexScanNode.class, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
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
        IndexDefinition activeIndex = IndexDefinition.create("active-index", "active", BsonType.BOOLEAN);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'active': false}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        assertInstanceOf(IndexScanNode.class, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
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
        IndexDefinition activeIndex = IndexDefinition.create("active-index", "active", BsonType.BOOLEAN);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // $gt: false is equivalent to true because true > false
        PipelineNode plan = createExecutionPlan(metadata, "{'active': {'$gt': false}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
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
        IndexDefinition activeIndex = IndexDefinition.create("active-index", "active", BsonType.BOOLEAN);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // $lt: false returns empty because nothing is less than false
        PipelineNode plan = createExecutionPlan(metadata, "{'active': {'$lt': false}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(0, results.size());
        }
    }

    @Test
    void shouldQueryBooleanIndexWithLtTrue() {
        IndexDefinition activeIndex = IndexDefinition.create("active-index", "active", BsonType.BOOLEAN);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // $lt: true is equivalent to false because false < true
        PipelineNode plan = createExecutionPlan(metadata, "{'active': {'$lt': true}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
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
        IndexDefinition activeIndex = IndexDefinition.create("active-index", "active", BsonType.BOOLEAN);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'John', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'active': false}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'active': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Claire', 'active': false}")
        );

        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Comparing boolean field with string value returns empty (type mismatch)
        PipelineNode plan = createExecutionPlan(metadata, "{'active': {'$lt': 'true'}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(0, results.size());
        }
    }
}
