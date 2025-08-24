package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.bucket.pipeline.PipelineContext;
import com.kronotop.bucket.pipeline.PipelineExecutor;
import com.kronotop.bucket.pipeline.PipelineNode;
import com.kronotop.bucket.pipeline.PipelineRewriter;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PlannerContext;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

class PipelineRewriterTest extends BasePlanExecutorTest {

    @Test
    void test() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Burak'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Ufuk'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Burhan'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Sevinc'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PhysicalNode plan = planQueryAndOptimize(metadata, "{'age': {'$gt': 22}}");
        PipelineNode node = PipelineRewriter.rewrite(plan);

        PipelineExecutor executor = new PipelineExecutor(node);

        PlannerContext plannerContext = new PlannerContext();
        PlanExecutorConfig config = new PlanExecutorConfig(metadata, plan, plannerContext);
        config.setLimit(10);

        PipelineContext ctx = new PipelineContext(context, metadata);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            executor.run(tr, ctx);
        }
    }
}