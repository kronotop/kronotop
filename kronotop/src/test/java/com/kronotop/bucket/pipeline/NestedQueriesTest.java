package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NestedQueriesTest extends BasePipelineTest {

    @Test
    void testNestedAndOrQueryReturnsCorrectIntersection() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-investigation";

        // Create bucket metadata without any secondary indexes
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert the specific test documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Laptop\", \"quantity\": 10, \"price\": 1200, \"category\": \"electronics\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Headphones\", \"quantity\": 3, \"price\": 50, \"category\": \"electronics\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Book A\", \"quantity\": 7, \"price\": 12, \"category\": \"book\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Book B\", \"quantity\": 2, \"price\": 20, \"category\": \"book\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Pen\", \"quantity\": 30, \"price\": 2, \"category\": \"stationery\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Notebook\", \"quantity\": 15, \"price\": 6, \"category\": \"stationery\" }")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // FS-2 FS-1
            String complexQuery = """
                    {
                      "$and": [
                        {
                          "$or": [
                            { "quantity": { "$gt": 5 } },
                            { "price": { "$lt": 10 } }
                          ]
                        },
                        {
                          "$or": [
                            { "category": { "$eq": "book" } },
                            {
                              "$and": [
                                { "category": { "$eq": "electronics" } },
                                { "price": { "$gt": 100 } }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                    """;

            PipelineExecutor executor = createPipelineExecutorForQuery(metadata, complexQuery);
            PipelineContext ctx = createPipelineContext(metadata);

            Map<?, ByteBuffer> results = executor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                System.out.println("Result >> " + BSONUtil.fromBson(buffer.array()).toJson());
            }

            // Verify correct intersection: should return exactly Laptop and Book A
            assertEquals(2, results.size(),
                    "Nested AND/OR query should return exactly 2 documents (Laptop and Book A)");

            // Extract document names for verification
            List<String> resultNames = results.values().stream()
                    .map(buf -> BSONUtil.fromBson(buf.array()).getString("name"))
                    .sorted()
                    .toList();

            assertEquals(List.of("Book A", "Laptop"), resultNames,
                    "Result should contain exactly Laptop and Book A");
        }
    }
}
