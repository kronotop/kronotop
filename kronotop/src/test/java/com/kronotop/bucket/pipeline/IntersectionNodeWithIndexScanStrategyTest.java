package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import org.bson.BsonType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IntersectionNodeWithIndexScanStrategyTest extends BasePipelineTest {
    @Test
    @Disabled
    void testIntersectionWithTwoField() {
        final String TEST_BUCKET_NAME = "test-intersection-full-scan-strategy";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                System.out.println(BSONUtil.fromBson(buffer.array()).toJson());
            }

            assertEquals(2, results.size(), "Should return exactly 3 documents with age > 22");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }
}
