package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

class PipelineRewriterTest extends BasePlanExecutorTest {

    @Test
    void test() {
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        createIndex("test", ageIndex);
        BucketMetadata metadata = getBucketMetadata("test");
        PhysicalNode plan = planQueryAndOptimize(metadata, "{'age': {'$gt': 22}}");
        PipelineNode node = PipelineRewriter.rewrite(plan);
        System.out.println(node);

        PipelineExecutor executor = new PipelineExecutor(node);
        PipelineContext ctx = new PipelineContext(metadata);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            executor.run(tr, ctx);
        }
    }
}