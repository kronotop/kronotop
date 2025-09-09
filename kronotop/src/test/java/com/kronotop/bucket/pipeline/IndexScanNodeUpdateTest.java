package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.internal.VersionstampUtil;
import org.bson.BsonBoolean;
import org.bson.BsonString;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexScanNodeUpdateTest extends BasePipelineTest {
    @Test
    void testUpdateWithGreaterThanFilter() {
        final String TEST_BUCKET_NAME = "test-bucket-gt-set-field-scan";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        List<Versionstamp> updateResult;
        {
            PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
            UpdateOptions update = UpdateOptions.builder().set("name", new BsonString("Donald")).build();
            QueryOptions options = QueryOptions.builder().update(update).build();
            QueryContext updateCtx = new QueryContext(metadata, options, plan);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                updateResult = updateExecutor.execute(tr, updateCtx);
                tr.commit().join();
            }
        }

        List<String> expectedResult = List.of(
                "{\"age\": 23, \"name\": \"Donald\"}",
                "{\"age\": 25, \"name\": \"Donald\"}",
                "{\"age\": 35, \"name\": \"Donald\"}"
        );

        {
            List<String> actualResult = new ArrayList<>();
            for (Versionstamp versionstamp : updateResult) {
                String query = String.format("{'_id': {'$eq': '%s'}}", VersionstampUtil.base32HexEncode(versionstamp));
                List<String> result = runQueryOnBucket(metadata, query);
                actualResult.addAll(result);
            }
            assertEquals(expectedResult, actualResult);
        }

        {
            List<String> actualResult = runQueryOnBucket(metadata, "{'age': {'$gt': 22}}");
            assertEquals(expectedResult, actualResult);
        }
    }

    @Test
    void testUpdateWithGreaterThanFilterWithLimit() {
        final String TEST_BUCKET_NAME = "test-bucket-gt-set-field-scan-with-limit";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        {
            PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
            UpdateOptions update = UpdateOptions.builder().set("name", new BsonString("Donald")).build();
            QueryOptions options = QueryOptions.builder().limit(1).update(update).build();
            QueryContext updateCtx = new QueryContext(metadata, options, plan);

            while (true) {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    List<Versionstamp> result = updateExecutor.execute(tr, updateCtx);
                    if (result.isEmpty()) {
                        break;
                    }
                    tr.commit().join();
                }
            }
        }

        List<String> expectedResult = List.of(
                "{\"age\": 23, \"name\": \"Donald\"}",
                "{\"age\": 25, \"name\": \"Donald\"}",
                "{\"age\": 35, \"name\": \"Donald\"}"
        );

        List<String> actualResult = runQueryOnBucket(metadata, "{'age': {'$gt': 22}}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void testUpdateWithGreaterThanFilterWithLimitReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-gt-set-field-scan-with-limit-reverse";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        List<Versionstamp> updateResult = new ArrayList<>();
        {
            PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
            UpdateOptions update = UpdateOptions.builder().set("name", new BsonString("Donald")).build();
            QueryOptions options = QueryOptions.builder().limit(1).reverse(true).update(update).build();
            QueryContext updateCtx = new QueryContext(metadata, options, plan);

            while (true) {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    List<Versionstamp> result = updateExecutor.execute(tr, updateCtx);
                    updateResult.addAll(result);
                    if (result.isEmpty()) {
                        break;
                    }
                    tr.commit().join();
                }
            }
        }

        List<String> expectedResult = List.of(
                "{\"age\": 35, \"name\": \"Donald\"}",
                "{\"age\": 25, \"name\": \"Donald\"}",
                "{\"age\": 23, \"name\": \"Donald\"}"
        );

        {
            List<String> actualResult = new ArrayList<>();
            for (Versionstamp versionstamp : updateResult) {
                String query = String.format("{'_id': {'$eq': '%s'}}", VersionstampUtil.base32HexEncode(versionstamp));
                List<String> result = runQueryOnBucket(metadata, query);
                actualResult.addAll(result);
            }
            assertEquals(expectedResult, actualResult);
        }
    }

    @Test
    void testUpdateWithGreaterThanFilterWithDoubleIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-double-index-update";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 10, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 50, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 5,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        List<Versionstamp> updateResult;
        {
            PipelineNode plan = createExecutionPlan(metadata, "{'price': {'$gte': 25}}");
            UpdateOptions update = UpdateOptions.builder().set("visible", new BsonBoolean(true)).build();
            QueryOptions options = QueryOptions.builder().update(update).build();
            QueryContext updateCtx = new QueryContext(metadata, options, plan);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                updateResult = updateExecutor.execute(tr, updateCtx);
                tr.commit().join();
            }
        }

        {
            List<String> expectedResult = List.of(
                    "{\"price\": 25, \"quantity\": 5, \"category\": \"Furniture\", \"visible\": true}",
                    "{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\", \"visible\": true}",
                    "{\"price\": 45, \"quantity\": 65, \"category\": \"Food\", \"visible\": true}"
            );

            List<String> actualResult = new ArrayList<>();
            for (Versionstamp versionstamp : updateResult) {
                String query = String.format("{'_id': {'$eq': '%s'}}", VersionstampUtil.base32HexEncode(versionstamp));
                List<String> result = runQueryOnBucket(metadata, query);
                actualResult.addAll(result);
            }
            assertEquals(expectedResult, actualResult);
        }

        {
            List<String> expectedResult = List.of(
                    "{\"price\": 25, \"quantity\": 5, \"category\": \"Furniture\", \"visible\": true}",
                    "{\"price\": 20, \"quantity\": 10, \"category\": \"Book\"}",
                    "{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\", \"visible\": true}",
                    "{\"price\": 23, \"quantity\": 50, \"category\": \"Electronics\"}",
                    "{\"price\": 45, \"quantity\": 65, \"category\": \"Food\", \"visible\": true}"
            );
            List<String> actualResult = runQueryOnBucket(metadata, "{'quantity': {'$gt': 3}}");
            assertEquals(expectedResult, actualResult);
        }
    }

    @Test
    void testUnsetFieldWithGreaterThanFilter() {
        final String TEST_BUCKET_NAME = "test-bucket-gt-unset-field-scan";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        List<Versionstamp> updateResult;
        {
            PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
            UpdateOptions update = UpdateOptions.builder().unset("name").build();
            QueryOptions options = QueryOptions.builder().update(update).build();
            QueryContext updateCtx = new QueryContext(metadata, options, plan);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                updateResult = updateExecutor.execute(tr, updateCtx);
                tr.commit().join();
            }
        }

        List<String> expectedResult = List.of(
                "{\"age\": 23}",
                "{\"age\": 25}",
                "{\"age\": 35}"
        );

        {
            List<String> actualResult = new ArrayList<>();
            for (Versionstamp versionstamp : updateResult) {
                String query = String.format("{'_id': {'$eq': '%s'}}", VersionstampUtil.base32HexEncode(versionstamp));
                List<String> result = runQueryOnBucket(metadata, query);
                actualResult.addAll(result);
            }
            assertEquals(expectedResult, actualResult);
        }

        {
            List<String> actualResult = runQueryOnBucket(metadata, "{'age': {'$gt': 22}}");
            assertEquals(expectedResult, actualResult);
        }
    }

    @Test
    void testUnsetAndSetFieldsWithGreaterThanFilter() {
        final String TEST_BUCKET_NAME = "test-bucket-gt-unset-set-fields-scan";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        List<Versionstamp> updateResult;
        {
            PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
            UpdateOptions update = UpdateOptions.builder().unset("name").set("field", new BsonString("new-field")).build();
            QueryOptions options = QueryOptions.builder().update(update).build();
            QueryContext updateCtx = new QueryContext(metadata, options, plan);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                updateResult = updateExecutor.execute(tr, updateCtx);
                tr.commit().join();
            }
        }

        List<String> expectedResult = List.of(
                "{\"age\": 23, \"field\": \"new-field\"}",
                "{\"age\": 25, \"field\": \"new-field\"}",
                "{\"age\": 35, \"field\": \"new-field\"}"
        );

        {
            List<String> actualResult = new ArrayList<>();
            for (Versionstamp versionstamp : updateResult) {
                String query = String.format("{'_id': {'$eq': '%s'}}", VersionstampUtil.base32HexEncode(versionstamp));
                List<String> result = runQueryOnBucket(metadata, query);
                actualResult.addAll(result);
            }
            assertEquals(expectedResult, actualResult);
        }

        {
            List<String> actualResult = runQueryOnBucket(metadata, "{'age': {'$gt': 22}}");
            assertEquals(expectedResult, actualResult);
        }
    }

    private List<String> runQueryOnBucket(BucketMetadata metadata, String query) {
        List<String> actualResult = new ArrayList<>();
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        return actualResult;
    }
}
