package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import org.bson.BsonType;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RangeScanNodeBatchingTest extends BasePipelineTest {

    @Test
    void testBatchingWith200Documents() {
        final String TEST_BUCKET_NAME = "test-bucket-batching-200-docs";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 200 documents with ages 1-200
        List<byte[]> documents = new ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes("{'age': " + i + ", 'name': 'User" + i + "'}"));
        }

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: age >= 51 AND age <= 170 (should match 120 documents: ages 51, 52, ..., 170)
        String query = "{ 'age': { '$gte': 51, '$lte': 170 } }";
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        // Expected: 120 documents matching, 2 per batch = 60 batches
        int expectedMatches = 120;
        int batchSize = 2;
        int expectedBatches = expectedMatches / batchSize; // 60 batches

        Set<Integer> allAges = new LinkedHashSet<>();
        int totalDocuments = 0;
        int batchCount = 0;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Loop until no more results
            Map<Versionstamp, ByteBuffer> batch;
            do {
                batch = readExecutor.execute(tr, ctx);
                if (!batch.isEmpty()) {
                    batchCount++;

                    // Extract ages from this batch
                    for (ByteBuffer buffer : batch.values()) {
                        Document doc = BSONUtil.fromBson(buffer.array());
                        allAges.add(doc.getInteger("age"));
                        totalDocuments++;
                    }

                    // Verify batch size (should be 2 or less for the last batch)
                    assertTrue(batch.size() <= batchSize, "Batch size should not exceed limit");
                    if (totalDocuments < expectedMatches) {
                        assertEquals(batchSize, batch.size(), "All batches except the last should have exactly " + batchSize + " documents");
                    }
                }
            } while (!batch.isEmpty());
        }

        // Verify total results
        assertEquals(expectedMatches, totalDocuments, "Should have exactly " + expectedMatches + " matching documents");
        assertEquals(expectedBatches, batchCount, "Should have exactly " + expectedBatches + " batches");

        // Verify age range: should have ages 51, 52, ..., 170
        assertEquals(expectedMatches, allAges.size(), "Should have " + expectedMatches + " unique ages");
        assertEquals(51, allAges.iterator().next(), "First age should be 51");
        assertTrue(allAges.contains(170), "Should contain age 170");
        
        // Verify sequential ages from 51 to 170
        List<Integer> sortedAges = new ArrayList<>(allAges);
        for (int i = 0; i < sortedAges.size(); i++) {
            assertEquals(51 + i, sortedAges.get(i), "Ages should be sequential starting from 51");
        }
    }


    @Test
    void testInt32RangeWithMixedInputWithReverseAndLimit() {
        final String TEST_BUCKET_NAME = "test-int32-range-with-mixed-input-with-limit-and-reverse";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 11, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 10, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 11, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 50, 'name': 'Alison'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gte': 20, '$lte': 48}}");
        QueryOptions config = QueryOptions.builder().reverse(true).limit(2).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<List<String>> expectedResult = new ArrayList<>();

        expectedResult.add(Arrays.asList(
                "{\"age\": 30, \"name\": \"George\"}",
                "{\"age\": 20, \"name\": \"Claire\"}"
        ));

        expectedResult.add(Arrays.asList(
                "{\"age\": 20, \"name\": \"George\"}",
                "{\"age\": 20, \"name\": \"Alice\"}"
        ));

        expectedResult.add(List.of(
                "{\"age\": 20, \"name\": \"John\"}"
        ));

        int index = 0;
        while(true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
                if (results.isEmpty()) {
                    break;
                }
                List<String> intermediate = new ArrayList<>();
                for (ByteBuffer buffer : results.values()) {
                    intermediate.add(BSONUtil.fromBson(buffer.array()).toJson());
                }
                assertEquals(expectedResult.get(index), intermediate);
                index++;
            }
        }
        assertEquals(expectedResult.size(), index);
    }
}
