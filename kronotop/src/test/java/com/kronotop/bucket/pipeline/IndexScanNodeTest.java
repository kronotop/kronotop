package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.internal.VersionstampUtil;
import org.bson.BsonType;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexScanNodeTest extends BasePipelineTest {

    private static Stream<Arguments> provideComparisonOperatorTestCases() {
        return Stream.of(
                // INT32 tests
                Arguments.of("GT", BsonType.INT32, "age",
                        List.of("{\"age\": 10}", "{\"age\": 20}", "{\"age\": 30}", "{\"age\": 40}"),
                        "25", 2, "Should return 2 documents with age > 25"),
                Arguments.of("LT", BsonType.INT32, "age",
                        List.of("{\"age\": 10}", "{\"age\": 20}", "{\"age\": 30}", "{\"age\": 40}"),
                        "25", 2, "Should return 2 documents with age < 25"),
                Arguments.of("EQ", BsonType.INT32, "age",
                        List.of("{\"age\": 10}", "{\"age\": 20}", "{\"age\": 30}", "{\"age\": 20}"),
                        "20", 2, "Should return 2 documents with age = 20"),
                Arguments.of("GTE", BsonType.INT32, "age",
                        List.of("{\"age\": 10}", "{\"age\": 20}", "{\"age\": 30}", "{\"age\": 40}"),
                        "20", 3, "Should return 3 documents with age >= 20"),
                Arguments.of("LTE", BsonType.INT32, "age",
                        List.of("{\"age\": 10}", "{\"age\": 20}", "{\"age\": 30}", "{\"age\": 40}"),
                        "20", 2, "Should return 2 documents with age <= 20"),

                // INT64 tests
                Arguments.of("GT", BsonType.INT64, "timestamp",
                        List.of("{\"timestamp\": 1000000000}", "{\"timestamp\": 2000000000}", "{\"timestamp\": 3000000000}"),
                        "1500000000", 2, "Should return 2 documents with timestamp > 1500000000"),
                Arguments.of("EQ", BsonType.INT64, "timestamp",
                        List.of("{\"timestamp\": 1000000000}", "{\"timestamp\": 2000000000}", "{\"timestamp\": 1000000000}"),
                        "1000000000", 2, "Should return 2 documents with timestamp = 1000000000"),

                // DOUBLE tests
                Arguments.of("GT", BsonType.DOUBLE, "price",
                        List.of("{\"price\": 10.5}", "{\"price\": 20.7}", "{\"price\": 30.2}", "{\"price\": 40.9}"),
                        "25.0", 2, "Should return 2 documents with price > 25.0"),
                Arguments.of("LT", BsonType.DOUBLE, "price",
                        List.of("{\"price\": 10.5}", "{\"price\": 20.7}", "{\"price\": 30.2}", "{\"price\": 40.9}"),
                        "25.0", 2, "Should return 2 documents with price < 25.0"),
                Arguments.of("EQ", BsonType.DOUBLE, "price",
                        List.of("{\"price\": 10.5}", "{\"price\": 20.7}", "{\"price\": 20.7}", "{\"price\": 40.9}"),
                        "20.7", 2, "Should return 2 documents with price = 20.7"),

                // STRING tests
                Arguments.of("GT", BsonType.STRING, "name",
                        List.of("{\"name\": \"Alice\"}", "{\"name\": \"Bob\"}", "{\"name\": \"Charlie\"}", "{\"name\": \"David\"}"),
                        "\"Bob\"", 2, "Should return 2 documents with name > 'Bob'"),
                Arguments.of("LT", BsonType.STRING, "name",
                        List.of("{\"name\": \"Alice\"}", "{\"name\": \"Bob\"}", "{\"name\": \"Charlie\"}", "{\"name\": \"David\"}"),
                        "\"Charlie\"", 2, "Should return 2 documents with name < 'Charlie'"),
                Arguments.of("EQ", BsonType.STRING, "name",
                        List.of("{\"name\": \"Alice\"}", "{\"name\": \"Bob\"}", "{\"name\": \"Alice\"}", "{\"name\": \"David\"}"),
                        "\"Alice\"", 2, "Should return 2 documents with name = 'Alice'"),

                // BOOLEAN tests
                Arguments.of("EQ", BsonType.BOOLEAN, "active",
                        List.of("{\"active\": true}", "{\"active\": false}", "{\"active\": true}", "{\"active\": false}"),
                        "true", 2, "Should return 2 documents with active = true")

                // DECIMAL128 tests
                /*Arguments.of("GT", BsonType.DECIMAL128, "balance",
                        List.of("{\"balance\": {\"$numberDecimal\": \"100.50\"}}",
                                "{\"balance\": {\"$numberDecimal\": \"200.75\"}}",
                                "{\"balance\": {\"$numberDecimal\": \"300.25\"}}"),
                        "{\"$numberDecimal\": \"150.00\"}", 2, "Should return 2 documents with balance > 150.00"),
                Arguments.of("EQ", BsonType.DECIMAL128, "balance",
                        List.of("{\"balance\": {\"$numberDecimal\": \"100.50\"}}",
                                "{\"balance\": {\"$numberDecimal\": \"200.75\"}}",
                                "{\"balance\": {\"$numberDecimal\": \"100.50\"}}"),
                        "{\"$numberDecimal\": \"100.50\"}", 2, "Should return 2 documents with balance = 100.50")*/
        );
    }

    @Test
    void testWithPrimaryIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        List<Versionstamp> versionstamps = insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String query = String.format("{'_id': {'$gt': '%s'}}", VersionstampUtil.base32HexEncode(versionstamps.getFirst()));
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 3 documents with age > 22 (ages 23, 25, 35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22");

            // Verify the content of each returned document
            assertEquals(Set.of("Alice", "George", "Claire"), extractNamesFromResults(results));
            assertEquals(Set.of(23, 25, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void testGtWithNegativeIntegers() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("negative-number-index", "negative", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'negative': -20}"),
                BSONUtil.jsonToDocumentThenBytes("{'negative': -23}"),
                BSONUtil.jsonToDocumentThenBytes("{'negative': -25}"),
                BSONUtil.jsonToDocumentThenBytes("{'negative': -35}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'negative': {'$gt': -26}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size());

            // Verify the content of each returned document
            assertEquals(Set.of(-25, -23, -20), extractIntegerFieldFromResults(results, "negative"));
        }
    }

    @Test
    void testLtWithNegativeIntegers() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("negative-number-index", "negative", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'negative': -20}"),
                BSONUtil.jsonToDocumentThenBytes("{'negative': -23}"),
                BSONUtil.jsonToDocumentThenBytes("{'negative': -25}"),
                BSONUtil.jsonToDocumentThenBytes("{'negative': -35}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'negative': {'$lte': -23}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size());

            // Verify the content of each returned document
            assertEquals(Set.of(-35, -25, -23), extractIntegerFieldFromResults(results, "negative"));
        }
    }

    @Test
    void testGtOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 3 documents with age > 22 (ages 23, 25, 35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22");

            // Verify the content of each returned document
            assertEquals(Set.of("Alice", "George", "Claire"), extractNamesFromResults(results));
            assertEquals(Set.of(23, 25, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void testEqOperatorWithReverseLimit_INT32() {
        final String TEST_BUCKET_NAME = "test-eq-operator-with-reverse-limit-int32";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alienor'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Calvin'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$eq': 20 } }");
        QueryOptions config = QueryOptions.builder().limit(2).reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<List<String>> expectedResult = new ArrayList<>();
        expectedResult.add(Arrays.asList(
                "{\"age\": 20, \"name\": \"Claire\"}",
                "{\"age\": 20, \"name\": \"Calvin\"}"
        ));

        expectedResult.add(Arrays.asList(
                "{\"age\": 20, \"name\": \"Alice\"}",
                "{\"age\": 20, \"name\": \"Alienor\"}"
        ));

        expectedResult.add(Arrays.asList(
                "{\"age\": 20, \"name\": \"George\"}",
                "{\"age\": 20, \"name\": \"John\"}"
        ));

        expectedResult.add(List.of(
                "{\"age\": 20, \"name\": \"Donald\"}"
        ));

        checkBatchedResultSet(ctx, expectedResult);
    }

    @Test
    void testEqOperatorWithReverseLimit_STRING() {
        final String TEST_BUCKET_NAME = "test-eq-operator-with-reverse-limit-string";

        // Create an age index for this test
        IndexDefinition nameIndex = IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, nameIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 21, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 22, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 24, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 26, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 27, 'name': 'John'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'name': {'$eq': 'John' } }");
        QueryOptions config = QueryOptions.builder().limit(2).reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<List<String>> expectedResult = new ArrayList<>();

        expectedResult.add(Arrays.asList(
                "{\"age\": 27, \"name\": \"John\"}",
                "{\"age\": 26, \"name\": \"John\"}"
        ));

        expectedResult.add(Arrays.asList(
                "{\"age\": 25, \"name\": \"John\"}",
                "{\"age\": 24, \"name\": \"John\"}"
        ));

        expectedResult.add(Arrays.asList(
                "{\"age\": 23, \"name\": \"John\"}",
                "{\"age\": 22, \"name\": \"John\"}"
        ));

        expectedResult.add(List.of(
                "{\"age\": 21, \"name\": \"John\"}"
        ));

        checkBatchedResultSet(ctx, expectedResult);
    }

    @Test
    void testNeOperatorReverseFilterWithLimit() {
        final String TEST_BUCKET_NAME = "test-ne-operator-reverse-filter-with-limit";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 21, 'name': 'Alienor'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Calvin'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$ne': 21 } }");
        QueryOptions config = QueryOptions.builder().limit(2).reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<List<String>> expectedResult = new ArrayList<>();

        expectedResult.add(Arrays.asList(
                "{\"age\": 20, \"name\": \"Claire\"}",
                "{\"age\": 20, \"name\": \"Calvin\"}"
        ));

        expectedResult.add(Arrays.asList(
                "{\"age\": 20, \"name\": \"Alice\"}",
                "{\"age\": 20, \"name\": \"George\"}"
        ));

        expectedResult.add(Arrays.asList(
                "{\"age\": 20, \"name\": \"John\"}",
                "{\"age\": 20, \"name\": \"Donald\"}"
        ));

        checkBatchedResultSet(ctx, expectedResult);
    }

    @Test
    void testIndexWithDoubleMaxValue() {
        final String TEST_BUCKET_NAME = "test-bucket-index-with-double-max-value";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("double-index", "double", BsonType.DOUBLE, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        String document = String.format("{\"double\": %s, \"string\": \"John\"}", Double.MAX_VALUE);
        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(BSONUtil.jsonToDocumentThenBytes(document));

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'double': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size());
            for (ByteBuffer buffer : results.values()) {
                assertEquals(document, BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
    }

    @Test
    void testIndexWithInt64MaxValue() {
        final String TEST_BUCKET_NAME = "test-bucket-index-with-long-max-value";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("long-index", "long", BsonType.INT64, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        String document = String.format("{\"long\": %s, \"string\": \"John\"}", Long.MAX_VALUE);
        System.out.println(document);
        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(BSONUtil.jsonToDocumentThenBytes(document));

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'long': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size());
            for (ByteBuffer buffer : results.values()) {
                assertEquals(document, BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
    }

    @Test
    void testIndexWithInt32MaxValue() {
        final String TEST_BUCKET_NAME = "test-bucket-index-with-integer-max-value";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("integer-index", "integer", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        String document = String.format("{\"integer\": %s, \"string\": \"John\"}", Integer.MAX_VALUE);
        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(BSONUtil.jsonToDocumentThenBytes(document));

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'integer': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size());
            for (ByteBuffer buffer : results.values()) {
                assertEquals(document, BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
    }

    @Test
    void testGtOperatorReturnsEmptyResultSet() {
        final String TEST_BUCKET_NAME = "test-bucket-empty-result-gt";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert documents with ages all below the query threshold
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 18, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 21, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 19, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for age > 22, which should match no documents since all ages are <= 21
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 0 documents since no documents have age > 22
            assertEquals(0, results.size(), "Should return exactly 0 documents with age > 22");
        }
    }

    @Test
    void testNeOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-ne";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different ages, including some with age 25
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Bob'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for age != 25, which should match 3 documents (ages 20, 30, 35)
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$ne': 25}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 3 documents with age != 25 (ages 20, 30, 35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age != 25");

            // Verify the content of each returned document
            assertEquals(Set.of("John", "George", "Bob"), extractNamesFromResults(results));
            assertEquals(Set.of(20, 30, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @ParameterizedTest
    @MethodSource("provideComparisonOperatorTestCases")
    void testComparisonOperatorsWithAllTypes(String operator, BsonType bsonType, String fieldName,
                                             List<String> testDocuments, String queryValue,
                                             int expectedCount, String testDescription) {
        final String TEST_BUCKET_NAME = "test-bucket-comparison-" + operator.toLowerCase() + "-" + bsonType.name().toLowerCase();

        // Create index for the test field
        IndexDefinition index = IndexDefinition.create(fieldName + "-index", fieldName, bsonType, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, index);

        // Insert test documents
        List<byte[]> documents = testDocuments.stream()
                .map(BSONUtil::jsonToDocumentThenBytes)
                .toList();
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Execute query
        String query = String.format("{'%s': {'$%s': %s}}", fieldName, operator.toLowerCase(), queryValue);
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(expectedCount, results.size(), testDescription);

            // Verify concrete expected results based on specific test cases
            if (!results.isEmpty()) {
                List<Object> actualFieldValues = new ArrayList<>();
                for (ByteBuffer buffer : results.values()) {
                    Document doc = BSONUtil.fromBson(buffer.array());
                    actualFieldValues.add(doc.get(fieldName));
                }

                // Check concrete expected results for specific test cases
                validateIndexScanResults(operator, bsonType, fieldName, testDocuments, queryValue,
                        actualFieldValues, testDescription, false);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideComparisonOperatorTestCases")
    void testComparisonOperatorsWithAllTypesReverse(String operator, BsonType bsonType, String fieldName,
                                                    List<String> testDocuments, String queryValue,
                                                    int expectedCount, String testDescription) {
        final String TEST_BUCKET_NAME = "test-bucket-comparison-reverse-" + operator.toLowerCase() + "-" + bsonType.name().toLowerCase();

        // Create index for the test field
        IndexDefinition index = IndexDefinition.create(fieldName + "-index", fieldName, bsonType, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, index);

        // Insert test documents
        List<byte[]> documents = testDocuments.stream()
                .map(BSONUtil::jsonToDocumentThenBytes)
                .toList();
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Execute query with REVERSE=true
        String query = String.format("{'%s': {'$%s': %s}}", fieldName, operator.toLowerCase(), queryValue);
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(expectedCount, results.size(), testDescription + " (REVERSE=true)");

            // Verify concrete expected results based on specific test cases
            if (!results.isEmpty()) {
                List<Object> actualFieldValues = new ArrayList<>();
                for (ByteBuffer buffer : results.values()) {
                    Document doc = BSONUtil.fromBson(buffer.array());
                    actualFieldValues.add(doc.get(fieldName));
                }

                // Check concrete expected results for specific test cases
                validateIndexScanResults(operator, bsonType, fieldName, testDocuments, queryValue,
                        actualFieldValues, testDescription + " (REVERSE=true)", true);
            }
        }
    }

    private void validateIndexScanResults(String operator, BsonType bsonType, String fieldName,
                                          List<String> testDocuments, String queryValue,
                                          List<Object> actualFieldValues, String testDescription, boolean isReverse) {
        // Calculate expected results based on the test data and operator
        List<Object> expectedValues = new ArrayList<>();

        // Parse the test documents to get all field values
        List<Object> allFieldValues = new ArrayList<>();
        for (String docJson : testDocuments) {
            Document doc = Document.parse(docJson);
            allFieldValues.add(doc.get(fieldName));
        }

        // Parse the query value for comparison
        Object queryVal = parseQueryValue(queryValue, bsonType);

        // Apply the operator logic to filter values
        for (Object fieldValue : allFieldValues) {
            if (shouldIncludeValue(fieldValue, queryVal, operator)) {
                expectedValues.add(fieldValue);
            }
        }

        // Sort expected values (ascending for forward, descending for reverse)
        expectedValues.sort((a, b) -> {
            int comparison = compareValues(a, b);
            return isReverse ? -comparison : comparison;
        });

        // Validate results
        assertEquals(expectedValues.size(), actualFieldValues.size(),
                "Expected " + expectedValues.size() + " values but got " + actualFieldValues.size() +
                        " for: " + testDescription);

        // Check each value matches
        for (int i = 0; i < expectedValues.size(); i++) {
            Object expected = expectedValues.get(i);
            Object actual = actualFieldValues.get(i);

            // Handle numeric type conversion issues
            if (expected instanceof Number && actual instanceof Number) {
                double expectedDouble = ((Number) expected).doubleValue();
                double actualDouble = ((Number) actual).doubleValue();
                assertEquals(expectedDouble, actualDouble, 0.001,
                        "At position " + i + ", expected " + expected + " but got " + actual +
                                " for: " + testDescription);
            } else {
                assertEquals(expected, actual,
                        "At position " + i + ", expected " + expected + " but got " + actual +
                                " for: " + testDescription);
            }
        }
    }

    private Object parseQueryValue(String queryValue, BsonType bsonType) {
        switch (bsonType) {
            case INT32:
                return Integer.parseInt(queryValue);
            case INT64:
                return Long.parseLong(queryValue);
            case DOUBLE:
                return Double.parseDouble(queryValue);
            case STRING:
                // Remove quotes from string values
                return queryValue.replaceAll("\"", "");
            case BOOLEAN:
                return Boolean.parseBoolean(queryValue);
            case DECIMAL128:
                // For decimal128, handle different input formats
                if (queryValue.contains("$numberDecimal")) {
                    // Parse from JSON format like {"$numberDecimal": "100.50"}
                    try {
                        Document decimalDoc = Document.parse(queryValue);
                        String decimalStr = decimalDoc.getString("$numberDecimal");
                        return Double.parseDouble(decimalStr);
                    } catch (Exception e) {
                        // If document parsing fails, try to extract the value directly
                        String extracted = queryValue.replaceAll(".*\"\\$numberDecimal\"\\s*:\\s*\"([^\"]+)\".*", "$1");
                        return Double.parseDouble(extracted);
                    }
                } else {
                    // Direct numeric value
                    return Double.parseDouble(queryValue);
                }
            default:
                return queryValue;
        }
    }

    private boolean shouldIncludeValue(Object fieldValue, Object queryValue, String operator) {
        int comparison = compareValues(fieldValue, queryValue);

        return switch (operator.toUpperCase()) {
            case "GT" -> comparison > 0;
            case "LT" -> comparison < 0;
            case "GTE" -> comparison >= 0;
            case "LTE" -> comparison <= 0;
            case "EQ" -> comparison == 0;
            case "NE" -> comparison != 0;
            default -> false;
        };
    }

    @SuppressWarnings("unchecked")
    private int compareValues(Object a, Object b) {
        // Handle DECIMAL128 values specially
        if (a != null && a.getClass().getSimpleName().equals("Decimal128")) {
            double aDouble = convertDecimal128ToDouble(a);
            double bDouble = (b instanceof Number) ? ((Number) b).doubleValue() : convertDecimal128ToDouble(b);
            return Double.compare(aDouble, bDouble);
        } else if (b != null && b.getClass().getSimpleName().equals("Decimal128")) {
            double aDouble = (a instanceof Number) ? ((Number) a).doubleValue() : convertDecimal128ToDouble(a);
            double bDouble = convertDecimal128ToDouble(b);
            return Double.compare(aDouble, bDouble);
        } else if (a instanceof Number && b instanceof Number) {
            double aDouble = ((Number) a).doubleValue();
            double bDouble = ((Number) b).doubleValue();
            return Double.compare(aDouble, bDouble);
        } else if (a instanceof String && b instanceof String) {
            return ((String) a).compareTo((String) b);
        } else if (a instanceof Boolean && b instanceof Boolean) {
            return ((Boolean) a).compareTo((Boolean) b);
        } else if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable<Object>) a).compareTo(b);
        }
        return 0;
    }

    private double convertDecimal128ToDouble(Object decimal128) {
        if (decimal128 == null) return 0.0;

        // Use reflection to get the value from Decimal128
        try {
            // Decimal128 has a doubleValue() method
            java.lang.reflect.Method doubleValueMethod = decimal128.getClass().getMethod("doubleValue");
            return (Double) doubleValueMethod.invoke(decimal128);
        } catch (Exception e) {
            // If reflection fails, try toString and parse
            try {
                return Double.parseDouble(decimal128.toString());
            } catch (NumberFormatException nfe) {
                return 0.0;
            }
        }
    }

    private void checkBatchedResultSet(QueryContext ctx, List<List<String>> expectedResult) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            int index = 0;
            while (true) {
                Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
                if (results.isEmpty()) {
                    break;
                }
                List<String> resultSet = new ArrayList<>();
                for (ByteBuffer buffer : results.values()) {
                    resultSet.add(BSONUtil.fromBson(buffer.array()).toJson());
                }
                List<String> expectedResultSet = expectedResult.get(index);
                assertEquals(expectedResultSet, resultSet);
                index++;
            }
            assertEquals(index, expectedResult.size());
        }
    }
}
