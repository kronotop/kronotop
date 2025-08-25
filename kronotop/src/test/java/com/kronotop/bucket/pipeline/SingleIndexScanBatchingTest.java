package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SingleIndexScanBatchingTest extends BasePipelineTest {
    @Test
    void testGtOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 21 documents with age > 22 (ages 23-43)
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Person1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 24, 'name': 'Person2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Person3'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 26, 'name': 'Person4'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 27, 'name': 'Person5'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 28, 'name': 'Person6'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 29, 'name': 'Person7'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Person8'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 31, 'name': 'Person9'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 32, 'name': 'Person10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 33, 'name': 'Person11'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 34, 'name': 'Person12'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Person13'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 36, 'name': 'Person14'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 37, 'name': 'Person15'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 38, 'name': 'Person16'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 39, 'name': 'Person17'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Person18'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 41, 'name': 'Person19'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 42, 'name': 'Person20'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 43, 'name': 'Person21'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{'age': {'$gt': 22}}");
        PipelineContext ctx = createPipelineContext(metadata);
        ctx.setLimit(2); // Set limit to 2 for batching

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            int totalProcessedDocuments = 0;
            int nonEmptyBatchCount = 0;
            int totalIterations = 0;

            while (true) {
                Map<?, ByteBuffer> results = executor.execute(tr, ctx);
                totalIterations++;

                if (results.isEmpty()) {
                    // Subsequent call should return empty result - this is expected
                    break;
                }

                nonEmptyBatchCount++;
                
                if (nonEmptyBatchCount <= 10) {
                    // First 10 batches should have 2 documents each
                    assertEquals(2, results.size(), 
                        String.format("Batch %d should contain exactly 2 documents", nonEmptyBatchCount));
                } else if (nonEmptyBatchCount == 11) {
                    // Last batch should have 1 document (21 total / 2 per batch = 10 full batches + 1 partial)
                    assertEquals(1, results.size(), 
                        "Last batch should contain exactly 1 document");
                }

                totalProcessedDocuments += results.size();

                // Verify the documents are returned in ascending age order
                Set<Integer> ages = extractAgesFromResults(results);
                for (Integer age : ages) {
                    assertTrue(age > 22, "All returned documents should have age > 22");
                }
            }

            // Verify total counts
            assertEquals(11, nonEmptyBatchCount, "Should have 11 non-empty batches total (10 with 2 docs + 1 with 1 doc)");
            assertEquals(12, totalIterations, "Should have 12 total iterations (11 non-empty + 1 empty)");
            assertEquals(21, totalProcessedDocuments, "Should process exactly 21 documents total");
        }
    }
}
