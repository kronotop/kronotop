package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

class PipelineRewriterTest extends BasePipelineTest {

    @Test
    void test() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition nameIndex = IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Burak'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Ufuk'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Burhan'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Sevinc'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PhysicalNode plan = planQueryAndOptimize(metadata, "{'age': {'$gt': 22}}");
        //PhysicalNode plan = planQueryAndOptimize(metadata, "{ 'age': {'$gt': 22}, 'name': {'$eq': 'Burhan'} }");
        PipelineNode node = PipelineRewriter.rewrite(plan);

        PipelineExecutor executor = new PipelineExecutor(node);

        IndexUtils indexUtils = new IndexUtils();
        CursorManager cursorManager = new CursorManager();
        SelectorCalculator selectorCalculator = new SelectorCalculator(indexUtils, cursorManager);
        DocumentRetriever documentRetriever = new DocumentRetriever(context.getService(BucketService.NAME));
        FilterEvaluator filterEvaluator = new FilterEvaluator();
        Dependencies dependencies = new Dependencies(selectorCalculator, documentRetriever, filterEvaluator, cursorManager);
        PipelineContext ctx = new PipelineContext(context, metadata, dependencies);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> results = executor.execute(tr, ctx);
            System.out.println(results);
        }
    }
}
// System.out.println("VERSIONSTAMP " + VersionstampUtil.base32HexEncode(location.versionstamp()) + " " + BSONUtil.fromBson(document.array()).toJson());