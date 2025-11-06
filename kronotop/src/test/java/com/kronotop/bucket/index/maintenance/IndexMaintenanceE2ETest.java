/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.*;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.statistics.HistogramBucket;
import com.kronotop.bucket.index.statistics.HistogramCodec;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTask;
import com.kronotop.bucket.index.statistics.IndexStatsBuilder;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IndexMaintenanceE2ETest extends BaseBucketHandlerTest {
    private static final String SKIP_WAIT_TRANSACTION_LIMIT_KEY =
            "__test__.index_maintenance.skip_wait_transaction_limit";

    @BeforeAll
    static void setUp() {
        System.setProperty(SKIP_WAIT_TRANSACTION_LIMIT_KEY, "false");
    }

    @AfterAll
    static void teardown() {
        System.clearProperty(SKIP_WAIT_TRANSACTION_LIMIT_KEY);
    }

    @Test
    void shouldTransitionIndexToReadyAfterBackgroundBuildCompletes() throws Exception {
        int halfway = 500;
        int totalInserts = 1000;

        CountDownLatch halfLatch = new CountDownLatch(halfway);
        CountDownLatch allLatch = new CountDownLatch(totalInserts);

        try (ExecutorService service = Executors.newSingleThreadExecutor()) {
            Future<?> bgFuture = service.submit(() -> insertAtBackground(halfLatch, allLatch, totalInserts));

            halfLatch.await();

            IndexDefinition definition = IndexDefinition.create(
                    "test-index",
                    "age",
                    BsonType.INT32,
                    IndexStatus.WAITING
            );

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
                tr.commit().join();
            }

            // Refresh the metadata
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

            allLatch.await();

            bgFuture.get();

            DirectorySubspace subspace = TransactionUtils.execute(context, tr ->
                    IndexUtil.open(tr, metadata.subspace(), definition.name()));
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    IndexDefinition indexDefinition = IndexUtil.loadIndexDefinition(tr, subspace);
                    return indexDefinition.status() == IndexStatus.READY;
                }
            });

            // All tasks must be dropped after this point
            AtomicInteger tasks = new AtomicInteger();
            BucketService bucketService = context.getService(BucketService.NAME);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (int shardId = 0; shardId < bucketService.getNumberOfShards(); shardId++) {
                    DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
                    TaskStorage.tasks(tr, taskSubspace, (taskId) -> {
                        tasks.getAndIncrement();
                        return true;
                    });
                }
            }
            assertEquals(0, tasks.getAndIncrement());
        }
    }

    @Test
    void shouldSweepStoppedBuildTasksForDroppedIndex() throws Exception {
        int halfway = 500;
        int totalInserts = 1000;

        CountDownLatch halfLatch = new CountDownLatch(halfway);
        CountDownLatch allLatch = new CountDownLatch(totalInserts);

        try (ExecutorService service = Executors.newSingleThreadExecutor()) {
            Future<?> bgFuture = service.submit(() -> insertAtBackground(halfLatch, allLatch, totalInserts));

            halfLatch.await();

            IndexDefinition definition = IndexDefinition.create(
                    "test-index",
                    "age",
                    BsonType.INT32,
                    IndexStatus.WAITING
            );

            TransactionUtils.executeThenCommit(context, tr -> {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
                return null;
            });

            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            final BucketMetadata finalMetadata = metadata;
            RetryMethods.retry(RetryMethods.TRANSACTION).executeRunnable(() -> {
                TransactionUtils.executeThenCommit(context, tr -> {
                    TransactionalContext tx = new TransactionalContext(context, tr);
                    IndexUtil.drop(tx, finalMetadata, definition.name());
                    return null;
                });
            });


            allLatch.await();

            bgFuture.get();

            BucketService bucketService = context.getService(BucketService.NAME);
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                // All build & drop tasks are killed and removed
                AtomicInteger counter = new AtomicInteger();
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    for (int shardId = 0; shardId < bucketService.getNumberOfShards(); shardId++) {
                        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
                        TaskStorage.tasks(tr, taskSubspace, (taskId) -> {
                            counter.getAndIncrement();
                            return true;
                        });
                    }
                }
                return counter.get() == 0;
            });

            // Refresh and check
            metadata = getBucketMetadata(TEST_BUCKET);
            assertNull(metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL));
        }
    }

    @Test
    void shouldDropAndWipeOutIndex() {
        // Insert documents with a simple field for filtering
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"value\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"B\", \"value\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"value\": 3}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"B\", \"value\": 4}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"value\": 5}")
        );

        insertDocuments(documents);

        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "value",
                BsonType.INT32
        );

        createIndexThenWaitForReadiness(definition);

        // Refresh the metadata
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.drop(tx, metadata, definition.name());
            tr.commit().join();
        }

        BucketService bucketService = context.getService(BucketService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            // All build & drop tasks are killed and removed
            AtomicInteger counter = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (int shardId = 0; shardId < bucketService.getNumberOfShards(); shardId++) {
                    DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
                    TaskStorage.tasks(tr, taskSubspace, (taskId) -> {
                        counter.getAndIncrement();
                        return true;
                    });
                }
            }
            return counter.get() == 0;
        });

        // Refresh and check
        metadata = getBucketMetadata(TEST_BUCKET);
        assertNull(metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL));
    }

    @Test
    void shouldBuildHistogramWithHints() {
        // Create index
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"numeric\": {\"name\": \"test-index\", \"bson_type\": \"int32\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        // Insert some data
        List<byte[]> documents = generateRandomDocumentsWithNumericContent("numeric", 1000);
        Map<String, byte[]> items = insertDocuments(documents, 50);

        // Wait until index is becoming ready to use
        IndexDefinition definition = loadIndexDefinition("numeric");
        assertNotNull(definition);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndex("numeric", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(index.subspace());
            waitUntilUpdated(metadata);
        }

        // Fill the hint space manually.
        List<String> randomKeys = selectRandomKeysFromMap(items, 200);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.READ);
            assertNotNull(index);
            for (String key : randomKeys) {
                IndexStatsBuilder.insertHintForStats(tr, VersionstampUtil.base32HexDecode(key), index);
            }
            tr.commit().join();
        }

        // Initiate an async analyze task
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexAnalyze(TEST_BUCKET, "test-index").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        // It should be built at background
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
                byte[] key = IndexUtil.histogramKey(metadata.subspace(), definition.id());
                byte[] value = tr.get(key).join();
                if (value == null) {
                    return false;
                }
                List<HistogramBucket> histogram = HistogramCodec.decode(value);
                return !histogram.isEmpty();
            }
        });
    }
}
