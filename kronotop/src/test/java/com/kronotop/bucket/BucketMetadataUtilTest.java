/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.CachedTimeService;
import com.kronotop.bucket.index.*;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.server.Session;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;


class BucketMetadataUtilTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldCreateOrOpenBucketMetadata() {
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, TEST_BUCKET);
        assertEquals(TEST_BUCKET, metadata.name());
        assertNotNull(metadata.subspace());
        assertNotNull(metadata.volumePrefix());
        Index index = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READONLY);
        assertNotNull(index);
        assertNotNull(index.subspace());
        assertTrue(metadata.version() > 0);
    }

    @Test
    void shouldReadVersionFromExistingBucketMetadata() {
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long version = BucketMetadataUtil.readVersion(tr, metadata.subspace());
            assertEquals(metadata.version(), version);
        }
    }

    @Test
    void shouldIncreaseVersion() {
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadataUtil.increaseVersion(tr, metadata.subspace(), IndexUtil.POSITIVE_DELTA_ONE);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long version = BucketMetadataUtil.readVersion(tr, metadata.subspace());
            // Increased by one
            assertEquals(metadata.version() + 1, version);
        }
    }

    @Test
    void shouldCreateOrOpenBucketMetadataConcurrently() throws InterruptedException {
        int threadCount = Runtime.getRuntime().availableProcessors();

        CountDownLatch checkpoint = new CountDownLatch(1);
        CountDownLatch latch = new CountDownLatch(threadCount);
        ConcurrentHashMap<Integer, BucketMetadata> result = new ConcurrentHashMap<>();

        class CreateOrOpenRunnable implements Runnable {
            private final int threadId;
            private final CountDownLatch latch;

            CreateOrOpenRunnable(CountDownLatch latch, int threadId) {
                this.threadId = threadId;
                this.latch = latch;
            }

            @Override
            public void run() {
                try {
                    // Wait until the end of the loop
                    checkpoint.await();

                    Session session = getSession();
                    BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, TEST_BUCKET);
                    result.put(threadId, metadata);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            }
        }
        for (int i = 0; i < threadCount; i++) {
            context.getVirtualThreadPerTaskExecutor().submit(new CreateOrOpenRunnable(latch, i));
        }

        checkpoint.countDown();

        latch.await();

        assertEquals(threadCount, result.size());

        // All versions must be the same
        HashSet<Long> versions = new HashSet<>();
        for (BucketMetadata metadata : result.values()) {
            versions.add(metadata.version());
        }
        assertEquals(1, versions.size());
    }

    @Test
    void shouldReadIndexStatistics() throws InterruptedException {
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, TEST_BUCKET);

        final IndexDefinition numericIndexDefinition = IndexDefinition.create(
                "numeric-index",
                "numeric-selector",
                BsonType.INT32
        );

        createIndexThenWaitForReadiness(numericIndexDefinition);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexUtil.mutateCardinality(tr, metadata.subspace(), numericIndexDefinition.id(), 1);
            tr.commit().join();
        }

        int concurrentIncrease = 10;
        CountDownLatch latch = new CountDownLatch(concurrentIncrease);

        class IndexStatisticsRunnable implements Runnable {
            @Override
            public void run() {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    IndexUtil.mutateCardinality(tr, metadata.subspace(), DefaultIndexDefinition.ID.id(), 1);
                    tr.commit().join();
                }
                latch.countDown();
            }
        }

        for (int i = 0; i < concurrentIncrease; i++) {
            context.getVirtualThreadPerTaskExecutor().submit(new IndexStatisticsRunnable());
        }

        latch.await();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Long, IndexStatistics> stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace());
            assertEquals(2, stats.size());

            IndexStatistics idIndexStats = stats.get(DefaultIndexDefinition.ID.id());
            assertEquals(concurrentIncrease, idIndexStats.cardinality());

            IndexStatistics numericIndexStats = stats.get(numericIndexDefinition.id());
            assertEquals(1, numericIndexStats.cardinality());
        }
    }

    @Test
    void shouldFetchIndexStatisticsFromFoundationDB() {
        final byte[] DOCUMENT = BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}");
        final int numberOfEntries = 10;
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        for (int i = 0; i < numberOfEntries; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(1), DOCUMENT).encode(buf);

            runCommand(instance.getChannel(), buf);
        }

        // This will flush all cached entries
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(context.getService(CachedTimeService.NAME), 0);
        cleanup.run();

        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, getSession(), TEST_BUCKET);
        IndexStatistics statistics = metadata.indexes().getStatistics(DefaultIndexDefinition.ID.id());
        assertEquals(numberOfEntries, statistics.cardinality());
    }

    @Test
    void shouldReadBucketMetadataHeader() {
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexUtil.mutateCardinality(tr, metadata.subspace(), DefaultIndexDefinition.ID.id(), 1);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadataHeader header = BucketMetadataUtil.readBucketMetadataHeader(tr, metadata.subspace());

            assertEquals(metadata.version(), header.version());
            assertEquals(1, header.indexStatistics().size());
            IndexStatistics numericIndexStats = header.indexStatistics().get(DefaultIndexDefinition.ID.id());
            assertEquals(1, numericIndexStats.cardinality());
        }
    }

    @Test
    void shouldRefreshIndexStatistics() {
        final byte[] DOCUMENT = BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}");
        final int numberOfEntries = 10;
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        for (int i = 0; i < numberOfEntries; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(1), DOCUMENT).encode(buf);

            runCommand(instance.getChannel(), buf);
        }

        {
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, getSession(), TEST_BUCKET);
            // Try hard refresh
            BucketMetadataUtil.refreshIndexStatistics(context, metadata, 0);
        }

        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, getSession(), TEST_BUCKET);
        IndexStatistics statistics = metadata.indexes().getStatistics(DefaultIndexDefinition.ID.id());
        assertEquals(numberOfEntries, statistics.cardinality());
    }

    @Test
    void shouldNotOpenNotExistingBucket() {
        Session session = getSession();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NoSuchBucketException exception = assertThrows(NoSuchBucketException.class, () -> {
                BucketMetadataUtil.open(context, tr, session, TEST_BUCKET);
            });
            assertEquals("No such bucket: 'test-bucket'", exception.getMessage());
        }
    }

    @Test
    void shouldOpenExistingBucket() {
        Session session = getSession();

        BucketMetadata expectedBucketMetadata = BucketMetadataUtil.createOrOpen(context, session, TEST_BUCKET);
        // This will flush all cached entries
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(context.getService(CachedTimeService.NAME), 0);
        cleanup.run();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = assertDoesNotThrow(() -> BucketMetadataUtil.open(context, tr, session, TEST_BUCKET));
            assertThat(metadata)
                    .usingRecursiveComparison()
                    .ignoringFields("indexes.lock",
                            "indexes.statistics",
                            "indexes.statsLastRefreshedAt")
                    .isEqualTo(expectedBucketMetadata);
        }
    }

    @Test
    void shouldReadIndexStatisticsForIndexId() {
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexUtil.mutateCardinality(tr, metadata.subspace(), DefaultIndexDefinition.ID.id(), 1);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), DefaultIndexDefinition.ID.id());
            assertEquals(1, stats.cardinality());
        }
    }
}