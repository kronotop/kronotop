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

import static org.junit.jupiter.api.Assertions.*;

public class AndOperatorWithIndexScanStrategyTest extends BasePipelineTest {
    @Test
    void testAndOperatorWithTwoIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-and-index-scan";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 100}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 120}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 80}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 140}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{ 'price': { '$gt': 22 }, 'quantity': { '$gt': 80 } }");
        PipelineContext ctx = createPipelineContext(metadata);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);

            // Expected results: documents with price > 22 AND quantity > 80
            // Document 1: {'price': 20, 'quantity': 100} - NO (price <= 22)
            // Document 2: {'price': 23, 'quantity': 120} - YES (price > 22 AND quantity > 80)
            // Document 3: {'price': 25, 'quantity': 80} - NO (quantity <= 80)
            // Document 4: {'price': 35, 'quantity': 140} - YES (price > 22 AND quantity > 80)

            assertEquals(2, results.size(), "Should return exactly 2 documents matching both conditions");

            // Verify the actual content of returned documents
            List<String> resultJsons = results.values().stream()
                    .map(buffer -> BSONUtil.fromBson(buffer.array()).toJson())
                    .sorted() // Sort for consistent comparison
                    .toList();

            // The two matching documents should have these price/quantity combinations
            boolean hasDoc23_120 = resultJsons.stream()
                    .anyMatch(json -> json.contains("\"price\": 23") && json.contains("\"quantity\": 120"));
            boolean hasDoc35_140 = resultJsons.stream()
                    .anyMatch(json -> json.contains("\"price\": 35") && json.contains("\"quantity\": 140"));

            assertTrue(hasDoc23_120, "Results should contain document with price=23 and quantity=120");
            assertTrue(hasDoc35_140, "Results should contain document with price=35 and quantity=140");

            // Verify no unwanted documents are included
            boolean hasDoc20_100 = resultJsons.stream()
                    .anyMatch(json -> json.contains("\"price\": 20") && json.contains("\"quantity\": 100"));
            boolean hasDoc25_80 = resultJsons.stream()
                    .anyMatch(json -> json.contains("\"price\": 25") && json.contains("\"quantity\": 80"));

            assertFalse(hasDoc20_100, "Results should not contain document with price=20 (fails price > 22)");
            assertFalse(hasDoc25_80, "Results should not contain document with quantity=80 (fails quantity > 80)");
        }
    }
}
