/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.index.TestIndexDefinitions;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class AndOperatorBatchAnalysisTest extends BasePipelineTest {
    private static final int DOCUMENT_COUNT = 350;
    private static final int BATCH_SIZE = 2;

    @Test
    void shouldReturnEachMatchingDocumentOnceAcrossBatches() {
        // Behavior: The bucket has INT32 indexes on price and quantity and holds 350 documents: 117 fail
        // price > 25, 117 fail quantity > 150 and 116 pass both. The test runs the query price > 25 and
        // quantity > 150 with batch size 2, one transaction per batch, until a batch comes back empty.
        // No batch has more than 2 documents, and each of the 116 matching documents comes back exactly once.
        final String TEST_BUCKET_NAME = "test-bucket-intersection-350-docs";

        SingleFieldIndexDefinition priceIndex = TestIndexDefinitions.singleField("price-index", "price", BsonType.INT32, false);
        SingleFieldIndexDefinition quantityIndex = TestIndexDefinitions.singleField("quantity-index", "quantity", BsonType.INT32, false);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = new ArrayList<>(DOCUMENT_COUNT);
        List<Integer> expectedIds = new ArrayList<>();
        for (int id = 0; id < DOCUMENT_COUNT; id++) {
            // Group 0 fails the price condition, group 1 fails the quantity condition, group 2 passes both.
            int group = id % 3;
            int price = group == 0 ? 1 + (id % 25) : 30 + (id % 50);
            int quantity = group == 1 ? 70 + (id % 80) : 160 + (id % 80);
            if (group == 2) {
                expectedIds.add(id);
            }
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d, 'id': %d}", price, quantity, id)));
        }
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ 'price': { '$gt': 25 }, 'quantity': { '$gt': 150 } }");
        QueryOptions config = QueryOptions.builder().batch(BATCH_SIZE).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        Set<Integer> seenIds = new HashSet<>();
        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                if (results.isEmpty()) {
                    break;
                }
                assertTrue(results.size() <= BATCH_SIZE, "A batch should hold at most " + BATCH_SIZE + " documents");

                for (ByteBuffer buffer : results) {
                    int id = TestUtil.BsonDocumentFromByteBuffer(buffer).getInt32("id").getValue();
                    assertTrue(seenIds.add(id), "Document with id " + id + " was returned twice");
                }
            }
        }

        assertEquals(expectedIds, seenIds.stream().sorted().toList());
    }
}
