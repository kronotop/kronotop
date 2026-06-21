/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.RetryMethods;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.statistics.HistogramBucket;
import com.kronotop.bucket.index.statistics.HistogramCodec;
import com.kronotop.bucket.index.statistics.IndexStatsBuilder;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.transaction.TransactionUtil;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

// Uses test-index-maintenance-watchdog.conf to set skip_wait_transaction_limit=false,
// which enables the convergence sleep in BucketMetadataConvergence. This is critical
// for reproducing production conditions: there is no global lock protecting index state
// transitions, so we rely on FDB's hard 5-second transaction limit plus a 5-second
// safety buffer (10 seconds total per convergence call). The boundary routine calls
// convergence twice — once for WAITING→BUILDING and once for BUILDING→READY — because
// each state transition must be fully visible to all in-flight insert transactions
// before the next phase begins. Without the sleep, the boundary captures the upper
// versionstamp while inserts are still in progress, and those inserts see stale
// (WAITING) metadata in the cache, so they are neither background-indexed nor
// synchronously maintained.
class IndexMaintenanceE2ETest extends BaseBucketHandlerTest {

    @Override
    protected String getConfigFileName() {
        return "test-index-maintenance-watchdog.conf";
    }

    @BeforeEach
    public void prepare() {
        createBucket(TEST_BUCKET);
    }

    private void checkCardinality(int numberOfIndexes, long expected) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Map<Long, IndexStatistics> statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            assertEquals(numberOfIndexes, statistics.size());
            for (IndexStatistics stats : statistics.values()) {
                assertEquals(expected, stats.cardinality());
            }
        }
    }

    // Verifies the background build over a window that overlaps synchronous writes. Coverage is the hard
    // guarantee: the secondary index holds exactly one entry per document. Cardinality is also exact: the
    // builder skips ObjectIds already indexed by the synchronous write path, so no document is counted
    // twice. The primary index is maintained synchronously only, so its cardinality stays exact too.
    private void checkBuildCoverageAndCardinality(String secondarySelector, long expected) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Map<Long, IndexStatistics> statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            assertEquals(2, statistics.size());

            Index primary = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.ALL);
            assertEquals(expected, statistics.get(primary.definition().id()).cardinality(),
                    "primary index cardinality must be exact");

            Index secondary = metadata.indexes().getIndex(secondarySelector, IndexSelectionPolicy.ALL);
            byte[] prefix = secondary.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
            long entryCount = tr.getRange(begin, end).asList().join().size();
            assertEquals(expected, entryCount, "secondary index must cover every document exactly once");

            long secondaryCardinality = statistics.get(secondary.definition().id()).cardinality();
            assertEquals(expected, secondaryCardinality,
                    "secondary index cardinality must be exact (dedup prevents double counting)");
        }
    }

    private void checkCardinalityFromMetadata(long expected, String... selectors) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            for (String selector : selectors) {
                Index index = metadata.indexes().getIndex(selector, IndexSelectionPolicy.READ);
                assertNotNull(index);
                IndexStatistics statistics = metadata.indexes().getStatistics(index.definition().id());
                assertEquals(expected, statistics.cardinality());
                assertTrue(statistics.histogram().isEmpty());
            }
        }
    }

    // Inserts `half` documents with age=32 and `half` documents with age=40 (2*half total).
    private int insertSplitAgeDocuments(int half) {
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < half; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes("{\"age\": 32}"));
            documents.add(BSONUtil.jsonToDocumentThenBytes("{\"age\": 40}"));
        }
        insertDocumentsAndGetObjectIds(documents, 50);
        return half * 2;
    }

    private void createAgeIndex() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "test-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }
    }

    private void awaitIndexStatus(DirectorySubspace indexSubspace, IndexStatus status) {
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return SingleFieldIndexUtil.loadIndexDefinition(tr, indexSubspace).status() == status;
            }
        });
    }

    private boolean isConflict(ErrorRedisMessage err) {
        String content = err.content();
        return content.contains("NOT_COMMITTED") || content.toLowerCase().contains("conflict");
    }

    // Issues a mutating command and returns its object_ids, retrying on transaction conflicts. The
    // builder's read-conflict range can abort a concurrent online write; a real client retries. Any
    // non-conflict error is a real failure and is surfaced immediately.
    private void mutateWithRetry(Supplier<ByteBuf> commandSupplier) {
        AtomicReference<Object> success = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Object resp = runCommand(channel, commandSupplier.get());
            if (resp instanceof ErrorRedisMessage err) {
                if (!isConflict(err)) {
                    throw new AssertionError("unexpected error from concurrent mutation: " + err.content());
                }
                return false;
            }
            success.set(resp);
            return true;
        });
        extractObjectIds(success.get());
    }

    // Churns the bucket while the index builds: repeatedly issues the given commands (round-robin) on a
    // background thread until `stop` is set, retrying each on transaction conflicts.
    private Future<?> startMutationLoop(ExecutorService service, AtomicBoolean stop, List<Supplier<ByteBuf>> commands) {
        return service.submit(() -> {
            int i = 0;
            while (!stop.get()) {
                mutateWithRetry(commands.get(i++ % commands.size()));
            }
        });
    }

    private DirectorySubspace ageIndexSubspace() {
        return refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET)
                .indexes().getIndex("age", IndexSelectionPolicy.ALL).subspace();
    }

    private void awaitAllTasksSwept() {
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            AtomicInteger counter = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (int shardId : context.getShardRegistry().getShardIds(ShardKind.BUCKET)) {
                    DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
                    TaskStorage.tasks(tr, taskSubspace, (ignored) -> {
                        counter.getAndIncrement();
                        return true;
                    });
                }
            }
            return counter.get() == 0;
        });
    }

    @Test
    void shouldKeepCardinalityExactWhenDocumentsDeletedDuringBackgroundBuild() throws Exception {
        // Behavior: When documents are repeatedly deleted while the secondary index is still building, the
        // surviving documents are each indexed exactly once and cardinality stays exact. Deleted documents
        // leave no phantom entry behind, even though the builder scans concurrently.
        int total = insertSplitAgeDocuments(500);

        createAgeIndex();
        DirectorySubspace indexSubspace = ageIndexSubspace();

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        Supplier<ByteBuf> deleteAge32 = () -> {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": 32}").encode(buf);
            return buf;
        };

        // Continuously delete the age=32 group while the build runs.
        AtomicBoolean stop = new AtomicBoolean(false);
        try (ExecutorService service = Executors.newSingleThreadExecutor()) {
            Future<?> loop = startMutationLoop(service, stop, List.of(deleteAge32));
            awaitIndexStatus(indexSubspace, IndexStatus.READY);
            awaitAllTasksSwept();
            stop.set(true);
            loop.get();
        }

        // Drain any age=32 document that survived the build window for a deterministic survivor count.
        mutateWithRetry(deleteAge32);

        // Only the untouched age=40 group remains, each indexed exactly once.
        checkBuildCoverageAndCardinality("age", total / 2);
    }

    @Test
    void shouldKeepCardinalityExactWhenDocumentsUpdatedDuringBackgroundBuild() throws Exception {
        // Behavior: When the indexed field is repeatedly updated while the secondary index is still
        // building, each document remains indexed exactly once under its current value and cardinality
        // stays exact. The builder skips ObjectIds already maintained online, so no stale entry under a
        // previous value lingers. No deletes occur, so every document stays indexed.
        int total = insertSplitAgeDocuments(500);

        createAgeIndex();
        DirectorySubspace indexSubspace = ageIndexSubspace();

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Continuously flip the indexed field 32<->99 on the same group while the build runs.
        AtomicBoolean stop = new AtomicBoolean(false);
        try (ExecutorService service = Executors.newSingleThreadExecutor()) {
            Future<?> loop = startMutationLoop(service, stop, List.of(
                    updateAge(cmd, 32, 99),
                    updateAge(cmd, 99, 32)
            ));
            awaitIndexStatus(indexSubspace, IndexStatus.READY);
            awaitAllTasksSwept();
            stop.set(true);
            loop.get();
        }

        checkBuildCoverageAndCardinality("age", total);
    }

    private Supplier<ByteBuf> updateAge(BucketCommandBuilder<byte[], byte[]> cmd, int from, int to) {
        return () -> {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": " + from + "}", "{\"$set\": {\"age\": " + to + "}}").encode(buf);
            return buf;
        };
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

            SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                    "test-index",
                    "age",
                    BsonType.INT32,
                    false,
                    IndexStatus.WAITING
            );

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
                tr.commit().join();
            }

            // Refresh the metadata
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

            allLatch.await();

            bgFuture.get();

            DirectorySubspace subspace = TransactionUtil.execute(context, tr ->
                    IndexUtil.open(tr, metadata.subspace(), definition.name()));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    SingleFieldIndexDefinition indexDefinition = SingleFieldIndexUtil.loadIndexDefinition(tr, subspace);
                    return indexDefinition.status() == IndexStatus.READY;
                }
            });

            // All tasks must be dropped after this point
            AtomicInteger tasks = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                List<Integer> shards = context.getShardRegistry().getShardIds(ShardKind.BUCKET);
                for (int shardId : shards) {
                    DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
                    TaskStorage.tasks(tr, taskSubspace, (ignored) -> {
                        tasks.getAndIncrement();
                        return true;
                    });
                }
            }
            assertEquals(0, tasks.getAndIncrement());
        }

        checkBuildCoverageAndCardinality("age", 2000);
    }

    @Test
    void shouldSweepStoppedBuildTasksAndCompleteIndexDrop() throws Exception {
        int halfway = 500;
        int totalInserts = 1000;

        CountDownLatch halfLatch = new CountDownLatch(halfway);
        CountDownLatch allLatch = new CountDownLatch(totalInserts);

        try (ExecutorService service = Executors.newSingleThreadExecutor()) {
            Future<?> bgFuture = service.submit(() -> insertAtBackground(halfLatch, allLatch, totalInserts));

            halfLatch.await();

            SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                    "test-index",
                    "age",
                    BsonType.INT32,
                    false,
                    IndexStatus.WAITING
            );

            TransactionUtil.executeThenCommit(context, tr -> {
                TransactionalContext tx = new TransactionalContext(context, tr);
                SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
                return null;
            });

            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            BucketMetadata finalMetadata = metadata;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                KronotopException exception = assertThrows(KronotopException.class,
                        () -> SingleFieldIndexUtil.drop(tx, finalMetadata, definition.name()));
                assertEquals("Index has active tasks", exception.getMessage());
            }

            Index index = metadata.indexes().getIndex("age", IndexSelectionPolicy.ALL);
            assertNotNull(index);

            // Stop the BUILD tasks
            RetryMethods.retry(RetryMethods.TRANSACTION).executeRunnable(() -> TransactionUtil.executeThenCommit(context, tr -> {
                IndexTaskUtil.scanTaskBackPointers(tr, index.subspace(), (taskId, shardId) -> {
                    DirectorySubspace tasksSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
                    IndexBuildingTaskState.setStatus(tr, tasksSubspace, taskId, IndexTaskStatus.STOPPED);
                    return true;
                });
                BucketMetadataUtil.publishBucketMetadataUpdatedEvent(new TransactionalContext(context, tr), finalMetadata);
                return null;
            }));

            waitUntilUpdated(metadata);

            // Create the drop task.
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    TransactionalContext tx = new TransactionalContext(context, tr);
                    SingleFieldIndexUtil.drop(tx, finalMetadata, definition.name());
                    tr.commit().join();
                } catch (Exception e) {
                    return false;
                }
                return true;
            });

            allLatch.await();

            bgFuture.get();

            await().atMost(Duration.ofSeconds(20)).until(() -> {
                // All build & drop tasks are killed and removed
                AtomicInteger counter = new AtomicInteger();
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    List<Integer> shards = context.getShardRegistry().getShardIds(ShardKind.BUCKET);
                    for (int shardId : shards) {
                        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
                        TaskStorage.tasks(tr, taskSubspace, (ignored) -> {
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

        insertDocumentsAndGetObjectIds(documents);

        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(
                "test-index",
                "value",
                BsonType.INT32
                , false, IndexStatus.WAITING);

        createIndexThenWaitForReadiness(definition);

        // Refresh the metadata
        BucketMetadata metadata = reloadBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            SingleFieldIndexUtil.drop(tx, metadata, definition.name());
            tr.commit().join();
        }

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            // All build & drop tasks are killed and removed
            AtomicInteger counter = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                List<Integer> shards = context.getShardRegistry().getShardIds(ShardKind.BUCKET);
                for (int shardId : shards) {
                    DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
                    TaskStorage.tasks(tr, taskSubspace, (ignored) -> {
                        counter.getAndIncrement();
                        return true;
                    });
                }
            }
            return counter.get() == 0;
        });

        // Refresh and check
        metadata = reloadBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
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
        Map<ObjectId, byte[]> items = insertDocumentsAndGetObjectIds(documents, 50);

        // Wait until index is becoming ready to use
        SingleFieldIndexDefinition definition = loadIndexDefinition("numeric");
        assertNotNull(definition);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndex("numeric", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(index.subspace());
            waitUntilUpdated(metadata);
        }

        // Fill the hint space manually.
        List<ObjectId> randomKeys = selectRandomKeysFromMap(items, 200);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.READ);
            assertNotNull(index);
            for (ObjectId objectId : randomKeys) {
                IndexStatsBuilder.setHintForStats(tr, index, objectId.toByteArray());
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

        // It should be built at the background
        await().atMost(Duration.ofSeconds(30)).until(() -> {
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

        checkBuildCoverageAndCardinality("numeric", 1000);
    }

    @Test
    void shouldTrackCardinalityForSynchronousIndexing() {
        // Create index
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"numeric\": {\"name\": \"test-index\", \"bson_type\": \"int32\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());

        // Wait until index is becoming ready to use
        SingleFieldIndexDefinition definition = loadIndexDefinition("numeric");
        assertNotNull(definition);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndex("numeric", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(index.subspace());
            waitUntilUpdated(metadata);
        }

        // Insert some data
        List<byte[]> documents = generateRandomDocumentsWithNumericContent("numeric", 1000);
        insertDocumentsAndGetObjectIds(documents, 50);

        checkCardinality(2, 1000);
        checkCardinalityFromMetadata(1000, "numeric", PrimaryIndex.SELECTOR);
    }

    @Test
    void shouldStoreCardinalityCorrectlyDuringBackgroundBuild() {
        // Create index
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"numeric\": {\"name\": \"test-index\", \"bson_type\": \"int32\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());

        // Insert some data
        List<byte[]> documents = generateRandomDocumentsWithNumericContent("numeric", 1000);
        insertDocumentsAndGetObjectIds(documents, 50);

        // Wait until the index is becoming ready to use
        SingleFieldIndexDefinition definition = loadIndexDefinition("numeric");
        assertNotNull(definition);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndex("numeric", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(index.subspace());
            waitUntilUpdated(metadata);
        }

        checkBuildCoverageAndCardinality("numeric", 1000);
        checkCardinalityFromMetadata(1000, PrimaryIndex.SELECTOR);
    }

    @Test
    void shouldTransitionCompoundIndexToReadyAfterBackgroundBuildCompletes() throws Exception {
        // Behavior: Creating a compound index after documents exist triggers background
        // building that transitions the index to READY and cleans up all tasks.

        int halfway = 500;
        int totalInserts = 1000;

        CountDownLatch halfLatch = new CountDownLatch(halfway);
        CountDownLatch allLatch = new CountDownLatch(totalInserts);

        try (ExecutorService service = Executors.newSingleThreadExecutor()) {
            Future<?> bgFuture = service.submit(() -> {
                BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
                byte[][] docs = makeDocumentsArray(List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"score\": 85}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"score\": 92}")
                ));
                for (int j = 0; j < totalInserts; j++) {
                    ByteBuf buf = Unpooled.buffer();
                    cmd.insert(TEST_BUCKET, docs).encode(buf);
                    runCommand(channel, buf);
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    halfLatch.countDown();
                    allLatch.countDown();
                }
            });

            halfLatch.await();

            CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                    "name-score-index",
                    List.of(
                            new CompoundIndexField("name", BsonType.STRING, false),
                            new CompoundIndexField("score", BsonType.INT32, false)
                    )
                    , IndexStatus.WAITING);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                CompoundIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
                tr.commit().join();
            }

            refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

            allLatch.await();
            bgFuture.get();

            // Wait for READY status
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
                return compoundIndex != null && compoundIndex.definition().status() == IndexStatus.READY;
            });

            // All tasks must be cleaned up
            AtomicInteger tasks = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                List<Integer> shards = context.getShardRegistry().getShardIds(ShardKind.BUCKET);
                for (int shardId : shards) {
                    DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
                    TaskStorage.tasks(tr, taskSubspace, (ignored) -> {
                        tasks.getAndIncrement();
                        return true;
                    });
                }
            }
            assertEquals(0, tasks.get());
        }
    }
}
