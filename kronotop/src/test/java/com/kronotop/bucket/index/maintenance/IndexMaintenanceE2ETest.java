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
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.internal.task.TaskStorage;
import org.bson.BsonType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
            BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

            allLatch.await();

            bgFuture.get();

            DirectorySubspace subspace = context.getFoundationDB().run(tr ->
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

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
                tr.commit().join();
            }

            BucketService bucketService = context.getService(BucketService.NAME);

            // Refresh the metadata
            BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.drop(tx, metadata, definition.name());
                tr.commit().join();
            }

            allLatch.await();

            bgFuture.get();

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
}
