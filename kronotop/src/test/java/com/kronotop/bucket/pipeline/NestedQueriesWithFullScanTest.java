package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NestedQueriesWithFullScanTest extends BasePipelineTest {

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

            PipelineNode plan = createExecutionPlan(metadata, complexQuery);
            QueryOptions config = QueryOptions.builder().build();
            QueryContext ctx = new QueryContext(metadata, config, plan);

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

    @Test
    void testNestedOrWithAndBranchesReturnsCorrectUnion() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-or-and";

        // Create bucket metadata without any secondary indexes
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert the specific test documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Phone\", \"quantity\": 2, \"price\": 800, \"category\": \"electronics\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"TV\", \"quantity\": 12, \"price\": 1500, \"category\": \"electronics\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Book C\", \"quantity\": 1, \"price\": 8, \"category\": \"book\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Book D\", \"quantity\": 9, \"price\": 22, \"category\": \"book\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Chair\", \"quantity\": 15, \"price\": 45, \"category\": \"furniture\" }")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String nestedOrQuery = """
                    {
                      "$or": [
                        {
                          "$and": [
                            { "category": { "$eq": "book" } },
                            { "quantity": { "$gt": 5 } }
                          ]
                        },
                        {
                          "$and": [
                            { "category": { "$eq": "electronics" } },
                            { "price": { "$lt": 1000 } }
                          ]
                        }
                      ]
                    }
                    """;
            PipelineNode plan = createExecutionPlan(metadata, nestedOrQuery);
            QueryOptions config = QueryOptions.builder().build();
            QueryContext ctx = new QueryContext(metadata, config, plan);

            List<String> expectedResult = List.of(
                    "{\"name\": \"Phone\", \"quantity\": 2, \"price\": 800, \"category\": \"electronics\"}",
                    "{\"name\": \"Book D\", \"quantity\": 9, \"price\": 22, \"category\": \"book\"}"
            );

            List<String> actualResult = new ArrayList<>();
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
            assertEquals(expectedResult, actualResult);
        }
    }

    @Test
    void testDeepNestedAndOrQueryWithMultipleFields() {
        final String TEST_BUCKET_NAME = "test-bucket-deep-nested";

        // Create bucket metadata without any secondary indexes
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert test documents with multiple fields for complex nested query testing
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Smartphone\", \"quantity\": 20, \"price\": 650, \"category\": \"electronics\", \"inStock\": true }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Tablet\", \"quantity\": 5, \"price\": 300, \"category\": \"electronics\", \"inStock\": false }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Science Book\", \"quantity\": 8, \"price\": 45, \"category\": \"book\", \"inStock\": true }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"History Book\", \"quantity\": 3, \"price\": 35, \"category\": \"book\", \"inStock\": true }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Sofa\", \"quantity\": 2, \"price\": 800, \"category\": \"furniture\", \"inStock\": false }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Table\", \"quantity\": 7, \"price\": 250, \"category\": \"furniture\", \"inStock\": true }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Marker Set\", \"quantity\": 40, \"price\": 15, \"category\": \"stationery\", \"inStock\": true }")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String deepNestedQuery = """
                    {
                      "$and": [
                        {
                          "$or": [
                            { "quantity": { "$gte": 8 } },
                            {
                              "$and": [
                                { "price": { "$lt": 50 } },
                                { "inStock": { "$eq": true } }
                              ]
                            }
                          ]
                        },
                        {
                          "$or": [
                            {
                              "$and": [
                                { "category": { "$eq": "electronics" } },
                                { "price": { "$gte": 400 } }
                              ]
                            },
                            { "category": { "$eq": "furniture" } }
                          ]
                        }
                      ]
                    }
                    """;

            PipelineNode plan = createExecutionPlan(metadata, deepNestedQuery);
            QueryOptions config = QueryOptions.builder().build();
            QueryContext ctx = new QueryContext(metadata, config, plan);

            Map<?, ByteBuffer> results = executor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                System.out.println("Result >> " + BSONUtil.fromBson(buffer.array()).toJson());
            }

            // Verify correct intersection: should return exactly Smartphone
            assertEquals(1, results.size(),
                    "Deep nested AND/OR query should return exactly 1 document (Smartphone)");

            // Extract document names for verification
            List<String> resultNames = results.values().stream()
                    .map(buf -> BSONUtil.fromBson(buf.array()).getString("name"))
                    .sorted()
                    .toList();

            assertEquals(List.of("Smartphone"), resultNames,
                    "Result should contain exactly Smartphone");

            /*
            DEEP NESTED QUERY ANALYSIS:

            First OR branch: quantity >= 8 OR (price < 50 AND inStock = true)
            - Smartphone: quantity=20 (✓) → Match ✓
            - Tablet: quantity=5 (✗), price=300 (✗) → No match
            - Science Book: quantity=8 (✓) → Match ✓
            - History Book: quantity=3 (✗), price=35 (✓) AND inStock=true (✓) → Match ✓
            - Sofa: quantity=2 (✗), price=800 (✗) → No match
            - Table: quantity=7 (✗), price=250 (✗) → No match
            - Marker Set: quantity=40 (✓) → Match ✓
            → First OR: {Smartphone, Science Book, History Book, Marker Set}

            Second OR branch: (category=electronics AND price>=400) OR category=furniture
            - Smartphone: category=electronics (✓) AND price=650 (✓) → Match ✓
            - Tablet: category=electronics (✓) AND price=300 (✗) → No match
            - Science Book: category=book (✗) → No match
            - History Book: category=book (✗) → No match
            - Sofa: category=furniture (✓) → Match ✓
            - Table: category=furniture (✓) → Match ✓
            - Marker Set: category=stationery (✗) → No match
            → Second OR: {Smartphone, Sofa, Table}

            Final AND intersection:
            {Smartphone, Science Book, History Book, Marker Set} ∩ {Smartphone, Sofa, Table}
            = {Smartphone}

            EXPECTED RESULT: Smartphone only
            */
        }
    }

    @Test
    void testNestedOrWithComplexAndBranchesReturnsCorrectUnion() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-or-complex";

        // Create bucket metadata without any secondary indexes
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert test documents with varied data for OR with AND branches testing
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Laptop Pro\", \"quantity\": 15, \"price\": 1200, \"category\": \"electronics\", \"brand\": \"TechCorp\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Mouse Pad\", \"quantity\": 100, \"price\": 8, \"category\": \"accessories\", \"brand\": \"OfficeMax\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Math Textbook\", \"quantity\": 12, \"price\": 85, \"category\": \"book\", \"brand\": \"EduPress\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Novel Collection\", \"quantity\": 6, \"price\": 25, \"category\": \"book\", \"brand\": \"ReadMore\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Gaming Chair\", \"quantity\": 3, \"price\": 450, \"category\": \"furniture\", \"brand\": \"ComfortMax\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Standing Desk\", \"quantity\": 8, \"price\": 320, \"category\": \"furniture\", \"brand\": \"WorkSpace\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Printer Paper\", \"quantity\": 200, \"price\": 12, \"category\": \"office\", \"brand\": \"PaperCo\" }")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String nestedOrWithAndQuery = """
                    {
                      "$or": [
                        {
                          "$and": [
                            { "category": { "$eq": "book" } },
                            { "price": { "$gt": 50 } }
                          ]
                        },
                        {
                          "$and": [
                            { "quantity": { "$gte": 100 } },
                            { "price": { "$lt": 20 } }
                          ]
                        },
                        {
                          "$and": [
                            { "category": { "$eq": "furniture" } },
                            { "brand": { "$eq": "WorkSpace" } }
                          ]
                        }
                      ]
                    }
                    """;

            PipelineNode plan = createExecutionPlan(metadata, nestedOrWithAndQuery);
            QueryOptions config = QueryOptions.builder().build();
            QueryContext ctx = new QueryContext(metadata, config, plan);

            Map<?, ByteBuffer> results = executor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                System.out.println("Result >> " + BSONUtil.fromBson(buffer.array()).toJson());
            }

            // Verify correct union: should return exactly Math Textbook, Mouse Pad, Printer Paper, and Standing Desk
            assertEquals(4, results.size(),
                    "Nested OR with AND branches should return exactly 4 documents");

            // Extract document names for verification
            List<String> resultNames = results.values().stream()
                    .map(buf -> BSONUtil.fromBson(buf.array()).getString("name"))
                    .sorted()
                    .toList();

            assertEquals(List.of("Math Textbook", "Mouse Pad", "Printer Paper", "Standing Desk"), resultNames,
                    "Result should contain exactly Math Textbook, Mouse Pad, Printer Paper, and Standing Desk");

            /*
            NESTED OR WITH COMPLEX AND BRANCHES ANALYSIS:

            First AND branch: category=book AND price>50
            - Laptop Pro: category=electronics (✗) → No match
            - Mouse Pad: category=accessories (✗) → No match
            - Math Textbook: category=book (✓) AND price=85 (✓) → Match ✓
            - Novel Collection: category=book (✓) AND price=25 (✗) → No match
            - Gaming Chair: category=furniture (✗) → No match
            - Standing Desk: category=furniture (✗) → No match
            - Printer Paper: category=office (✗) → No match
            → First AND: {Math Textbook}

            Second AND branch: quantity>=100 AND price<20
            - Laptop Pro: quantity=15 (✗) → No match
            - Mouse Pad: quantity=100 (✓) AND price=8 (✓) → Match ✓
            - Math Textbook: quantity=12 (✗) → No match
            - Novel Collection: quantity=6 (✗) → No match
            - Gaming Chair: quantity=3 (✗) → No match
            - Standing Desk: quantity=8 (✗) → No match
            - Printer Paper: quantity=200 (✓) AND price=12 (✓) → Match ✓
            → Second AND: {Mouse Pad, Printer Paper}

            Third AND branch: category=furniture AND brand=WorkSpace
            - Laptop Pro: category=electronics (✗) → No match
            - Mouse Pad: category=accessories (✗) → No match
            - Math Textbook: category=book (✗) → No match
            - Novel Collection: category=book (✗) → No match
            - Gaming Chair: category=furniture (✓) AND brand=ComfortMax (✗) → No match
            - Standing Desk: category=furniture (✓) AND brand=WorkSpace (✓) → Match ✓
            - Printer Paper: category=office (✗) → No match
            → Third AND: {Standing Desk}

            OR union: {Math Textbook} ∪ {Mouse Pad, Printer Paper} ∪ {Standing Desk}
            = {Math Textbook, Mouse Pad, Printer Paper, Standing Desk}

            EXPECTED RESULT: Math Textbook, Mouse Pad, Printer Paper, Standing Desk
            */
        }
    }

    @Test
    void testNestedQueryWithContradictoryAND() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-or-complex";

        // Create bucket metadata without any secondary indexes
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert test documents with varied data for OR with AND branches testing
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Laptop Pro\", \"quantity\": 15, \"price\": 1200, \"category\": \"electronics\", \"brand\": \"TechCorp\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Mouse Pad\", \"quantity\": 100, \"price\": 8, \"category\": \"accessories\", \"brand\": \"OfficeMax\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Math Textbook\", \"quantity\": 12, \"price\": 85, \"category\": \"book\", \"brand\": \"EduPress\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Novel Collection\", \"quantity\": 6, \"price\": 25, \"category\": \"book\", \"brand\": \"ReadMore\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Gaming Chair\", \"quantity\": 3, \"price\": 450, \"category\": \"furniture\", \"brand\": \"ComfortMax\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Standing Desk\", \"quantity\": 8, \"price\": 320, \"category\": \"furniture\", \"brand\": \"WorkSpace\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Printer Paper\", \"quantity\": 200, \"price\": 12, \"category\": \"office\", \"brand\": \"PaperCo\" }")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String nestedOrWithAndQuery = """
                    {
                      "$or": [
                        {
                          "$and": [
                            { "category": { "$eq": "book" } },
                            { "price": { "$gt": 50 } }
                          ]
                        },
                        {
                          "$and": [
                            { "quantity": { "$gte": 100 } },
                            { "price": { "$lt": 20 } }
                          ]
                        },
                        {
                          "$and": [
                            { "category": { "$eq": "furniture" } },
                            { "category": { "$ne": "furniture" } }
                          ]
                        }
                      ]
                    }
                    """;

            PipelineNode plan = createExecutionPlan(metadata, nestedOrWithAndQuery);
            QueryOptions config = QueryOptions.builder().build();
            QueryContext ctx = new QueryContext(metadata, config, plan);

            Map<?, ByteBuffer> results = executor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                System.out.println("Result >> " + BSONUtil.fromBson(buffer.array()).toJson());
            }

            // Verify correct union: should return exactly Math Textbook, Mouse Pad, Printer Paper
            assertEquals(3, results.size(),
                    "Nested OR with AND branches should return exactly 4 documents");

            // Extract document names for verification
            List<String> resultNames = results.values().stream()
                    .map(buf -> BSONUtil.fromBson(buf.array()).getString("name"))
                    .sorted()
                    .toList();

            assertEquals(List.of("Math Textbook", "Mouse Pad", "Printer Paper"), resultNames,
                    "Result should contain exactly Math Textbook, Mouse Pad and Printer Paper");
        }
    }
}