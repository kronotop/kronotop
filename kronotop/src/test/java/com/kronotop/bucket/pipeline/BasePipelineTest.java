package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseHandlerTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.optimizer.Optimizer;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PhysicalPlanner;
import com.kronotop.bucket.planner.physical.PlannerContext;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.server.Session;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonBinaryReader;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class BasePipelineTest extends BaseHandlerTest {

    protected static final String BUCKET_NAME = "test-bucket";
    protected static final int SHARD_ID = 0;

    private final LogicalPlanner logicalPlanner = new LogicalPlanner();
    private final PhysicalPlanner physicalPlanner = new PhysicalPlanner();
    private final Optimizer optimizer = new Optimizer();
    private final CursorManager cursorManager = new CursorManager();
    protected ReadExecutor readExecutor;
    protected DeleteExecutor deleteExecutor;

    @BeforeEach
    public void setupPipelineExecutor() {
        BucketService bucketService = context.getService(BucketService.NAME);
        DocumentRetriever documentRetriever = new DocumentRetriever(bucketService);
        PipelineEnv env = new PipelineEnv(bucketService, documentRetriever, cursorManager);
        PipelineExecutor executor = new PipelineExecutor(env);
        readExecutor = new ReadExecutor(executor);
        deleteExecutor = new DeleteExecutor(executor);
    }

    protected List<Versionstamp> insertDocumentsAndGetVersionstamps(String bucketName, List<byte[]> documents) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(bucketName, BucketInsertArgs.Builder.shard(SHARD_ID), documents).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(documents.size(), actualMessage.children().size(),
                String.format("Should have %d versionstamps for %d documents", documents.size(), documents.size()));

        List<Versionstamp> versionstamps = new ArrayList<>();
        for (int i = 0; i < documents.size(); i++) {
            SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().get(i);
            assertNotNull(message.content());
            Versionstamp versionstamp = assertDoesNotThrow(() -> VersionstampUtil.base32HexDecode(message.content()));
            versionstamps.add(versionstamp);
        }
        return versionstamps;
    }

    protected void createBucket(String bucketName) {
        // Bucket is created implicitly through BucketMetadataUtil.createOrOpen()
        Session session = getSession();
        BucketMetadataUtil.createOrOpen(context, session, bucketName);
    }

    void createIndex(Transaction tr, String bucket, IndexDefinition definition) {
        BucketMetadata metadata = getBucketMetadata(bucket);
        DirectorySubspace indexSubspace = IndexUtil.create(tr, metadata.subspace(), definition);
        assertNotNull(indexSubspace);
    }

    protected void createIndex(String bucketName, IndexDefinition indexDefinition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            createIndex(tr, bucketName, indexDefinition);
            tr.commit().join();
        }
    }

    protected BucketMetadata createIndexesAndLoadBucketMetadata(String bucketName, IndexDefinition... indexes) {
        // Create the bucket first
        createBucket(bucketName);

        // Create indexes
        for (IndexDefinition index : indexes) {
            createIndex(bucketName, index);
        }

        // Load and return metadata
        Session session = getSession();
        return BucketMetadataUtil.createOrOpen(context, session, bucketName);
    }

    protected PipelineNode createExecutionPlan(BucketMetadata metadata, String query) {
        PlannerContext plannerCtx = new PlannerContext();
        BqlExpr parsedQuery = BqlParser.parse(query);
        LogicalNode logicalPlan = logicalPlanner.planAndValidate(parsedQuery);
        PhysicalNode physicalPlan = physicalPlanner.plan(metadata, logicalPlan, plannerCtx);
        PhysicalNode optimizedPlan = optimizer.optimize(metadata, physicalPlan, plannerCtx);
        return PipelineRewriter.rewrite(plannerCtx, optimizedPlan);
    }


    // Helper method to extract names from results
    Set<String> extractNamesFromResults(Map<?, ByteBuffer> results) {
        Set<String> names = new HashSet<>();
        for (ByteBuffer documentBuffer : results.values()) {
            documentBuffer.rewind();
            try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();
                while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if ("name".equals(fieldName)) {
                        names.add(reader.readString());
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();
            }
        }
        return names;
    }

    Set<Integer> extractIntegerFieldFromResults(Map<?, ByteBuffer> results, String field) {
        Set<Integer> ages = new LinkedHashSet<>();

        for (ByteBuffer documentBuffer : results.values()) {
            documentBuffer.rewind();
            try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();

                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if (field.equals(fieldName)) {
                        ages.add(reader.readInt32());
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();
            }
        }

        return ages;
    }

    List<KeyValue> fetchAllIndexedEntries(DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    List<KeyValue> fetchAllIndexBackPointers(DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }
}
