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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AndOperatorWithIndexScanStrategyTest extends BasePipelineTest {
    @Test
    void testAndOperator() {
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

            for (ByteBuffer buffer : results.values()) {
                System.out.println(BSONUtil.fromBson(buffer.array()).toJson());
            }

            assertEquals(2, results.size(), "Should return exactly 1 document1");
            System.out.println(results);
        }
    }
}
