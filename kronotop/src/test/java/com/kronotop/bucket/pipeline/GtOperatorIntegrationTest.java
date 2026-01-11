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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.internal.VersionstampUtil;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class GtOperatorIntegrationTest extends BasePipelineTest {

    @Test
    void shouldHandleNotExistedField() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);
        PipelineNode plan = createExecutionPlan(metadata, "{'not-existed-field': {'$gt': 22}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void shouldFallbackToFullScanWhenPredicateTypeMismatchesIndexType() {
        final String TEST_BUCKET_NAME = "test-bucket-type-mismatch-fullscan";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': '20'}}");
        assertInstanceOf(FullScanNode.class, plan);

        FullScanNode fullScanNode = (FullScanNode) plan;
        assertEquals(DefaultIndexDefinition.ID, fullScanNode.getIndexDefinition());

        assertInstanceOf(ResidualPredicate.class, fullScanNode.predicate());
        ResidualPredicate residualPredicate = (ResidualPredicate) fullScanNode.predicate();
        assertEquals(Operator.GT, residualPredicate.op());
        assertEquals("age", residualPredicate.selector());
        assertInstanceOf(StringVal.class, residualPredicate.operand());
        StringVal stringVal = (StringVal) residualPredicate.operand();
        assertEquals("20", stringVal.value());
    }

    @Test
    void shouldReturnEmptyResultWhenQueryingIdGreaterThanMax() {
        final String TEST_BUCKET_NAME = "test-bucket-id-gt-max";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes("{'status': 'ALIVE'}"));
        }

        List<Versionstamp> versionstamps = insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);
        Versionstamp greatestId = versionstamps.getLast();

        String query = String.format("{'_id': {'$gt': '%s'}}", VersionstampUtil.base32HexEncode(greatestId));
        PipelineNode plan = createExecutionPlan(metadata, query);
        assertInstanceOf(IndexScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }
}
