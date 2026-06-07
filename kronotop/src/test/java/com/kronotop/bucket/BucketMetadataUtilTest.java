/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.index.*;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.UUIDUtil;
import com.kronotop.journal.Consumer;
import com.kronotop.journal.ConsumerConfig;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalName;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.kronotop.bucket.BucketMetadataUtil.POSITIVE_DELTA_ONE;
import static com.kronotop.bucket.BucketMetadataUtil.namespaceBindingKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;


class BucketMetadataUtilTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldPublishBucketMetadataUpdatedEvent() {
        // Behavior: publishBucketMetadataUpdatedEvent writes a journal event containing the bucket's namespace, name, id, and version.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            tr.commit().join();
        }
        ConsumerConfig cfg = new ConsumerConfig(
                UUID.randomUUID().toString(),
                JournalName.BUCKET_EVENTS.getValue(),
                ConsumerConfig.Offset.RESUME
        );
        Consumer consumer = new Consumer(context, cfg);
        try {
            consumer.start();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Event event = consumer.consume(tr);
                BucketMetadataUpdatedEvent evt = JSONUtil.readValue(event.value(), BucketMetadataUpdatedEvent.class);
                assertEquals(TEST_NAMESPACE, evt.namespace());
                assertEquals(TEST_BUCKET, evt.bucket());
                assertEquals(metadata.uuid(), evt.id());
                assertEquals(metadata.version(), evt.minimumVersion());
            }
        } finally {
            consumer.stop();
        }
    }

    @Test
    void shouldReadVersionFromExistingBucketMetadata() {
        // Behavior: readVersion returns the version that was set during bucket creation.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long version = BucketMetadataUtil.readVersion(tr, metadata.subspace());
            assertEquals(metadata.version(), version);
        }
    }

    @Test
    void shouldIncreaseVersion() {
        // Behavior: increaseVersion atomically increments the stored version by the given delta.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadataUtil.increaseVersion(tr, metadata.subspace(), POSITIVE_DELTA_ONE);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long version = BucketMetadataUtil.readVersion(tr, metadata.subspace());
            // Increased by one
            assertEquals(metadata.version() + 1, version);
        }
    }

    @Test
    void shouldReadIndexStatistics() throws InterruptedException {
        // Behavior: readIndexStatistics returns per-index cardinality after concurrent mutations across multiple indexes.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        final SingleFieldIndexDefinition numericIndexDefinition = SingleFieldIndexDefinition.create(
                "numeric-index",
                "numeric-selector",
                BsonType.INT32
                , false, IndexStatus.WAITING);

        createIndexThenWaitForReadiness(numericIndexDefinition);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexUtil.mutateCardinality(tr, metadata.subspace(), numericIndexDefinition.id(), 1);
            tr.commit().join();
        }

        int concurrentIncrease = 10;
        CountDownLatch latch = new CountDownLatch(concurrentIncrease);

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);

        class IndexStatisticsRunnable implements Runnable {
            @Override
            public void run() {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    IndexUtil.mutateCardinality(tr, metadata.subspace(), primaryIndex.definition().id(), 1);
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
            Map<Long, IndexStatistics> stats = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            assertEquals(2, stats.size());

            IndexStatistics idIndexStats = stats.get(primaryIndex.definition().id());
            assertEquals(concurrentIncrease, idIndexStats.cardinality());

            IndexStatistics numericIndexStats = stats.get(numericIndexDefinition.id());
            assertEquals(1, numericIndexStats.cardinality());
        }
    }

    @Test
    void shouldFetchIndexStatisticsFromFoundationDB() {
        // Behavior: After inserting documents and flushing the cache, opening the bucket loads fresh index statistics from FoundationDB.
        BucketMetadataUtil.create(context, getSession(), TEST_BUCKET, List.of(TEST_SHARD_ID));

        final byte[] DOCUMENT = BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}");
        final int numberOfEntries = 10;
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        for (int i = 0; i < numberOfEntries; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, DOCUMENT).encode(buf);

            runCommand(instance.getChannel(), buf);
        }

        // This will flush all cached entries
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(context::now, 0);
        cleanup.run();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, getSession(), TEST_BUCKET);
            Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
            IndexStatistics statistics = metadata.indexes().getStatistics(primaryIndex.definition().id());
            assertEquals(numberOfEntries, statistics.cardinality());
        }
    }

    @Test
    void shouldReadBucketMetadataHeader() {
        // Behavior: BucketMetadataHeader.read returns the version and per-index statistics from the header.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexUtil.mutateCardinality(tr, metadata.subspace(), primaryIndex.definition().id(), 1);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadataHeader header = BucketMetadataHeader.read(tr, metadata.subspace());

            assertEquals(metadata.version(), header.version());
            assertEquals(1, header.indexStatistics().size());
            IndexStatistics numericIndexStats = header.indexStatistics().get(primaryIndex.definition().id());
            assertEquals(1, numericIndexStats.cardinality());
        }
    }

    @Test
    void shouldRefreshIndexStatistics() {
        // Behavior: refreshIndexStatistics with ttl=0 forces a reload, making subsequent reads reflect current cardinality.
        BucketMetadataUtil.create(context, getSession(), TEST_BUCKET, List.of(TEST_SHARD_ID));

        final byte[] DOCUMENT = BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}");
        final int numberOfEntries = 10;
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        for (int i = 0; i < numberOfEntries; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, DOCUMENT).encode(buf);

            runCommand(instance.getChannel(), buf);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, getSession(), TEST_BUCKET);
            // Try hard refresh
            BucketMetadataUtil.refreshIndexStatistics(context, metadata, 0);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, getSession(), TEST_BUCKET);
            Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
            IndexStatistics statistics = metadata.indexes().getStatistics(primaryIndex.definition().id());
            assertEquals(numberOfEntries, statistics.cardinality());
        }
    }

    @Test
    void shouldNotOpenNotExistingBucket() {
        // Behavior: open throws NoSuchBucketException when the bucket does not exist.
        Session session = getSession();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NoSuchBucketException exception = assertThrows(NoSuchBucketException.class, () ->
                    BucketMetadataUtil.open(context, tr, session, TEST_BUCKET));
            assertEquals("No such bucket: 'test-bucket'", exception.getMessage());
        }
    }

    @Test
    void shouldOpenExistingBucket() {
        // Behavior: open returns metadata matching the originally created bucket after cache eviction.
        Session session = getSession();

        BucketMetadata expectedBucketMetadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));
        // This will flush all cached entries
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(context::now, 0);
        cleanup.run();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = assertDoesNotThrow(() -> BucketMetadataUtil.open(context, tr, session, TEST_BUCKET));
            assertThat(metadata)
                    .usingRecursiveComparison()
                    .ignoringFields("indexes.lock",
                            "indexes.statistics",
                            "compoundIndexes.statistics",
                            "compoundIndexes.statsLastRefreshedAt",
                            "indexes.statsLastRefreshedAt",
                            "compoundIndexes.lock",
                            "vectorIndexes.lock")
                    .isEqualTo(expectedBucketMetadata);
        }
    }

    @Test
    void shouldReadIndexStatisticsForIndexId() {
        // Behavior: readIndexStatistics for a specific index ID returns the cardinality after a mutation.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexUtil.mutateCardinality(tr, metadata.subspace(), primaryIndex.definition().id(), 1);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), primaryIndex.definition().id());
            assertEquals(1, stats.cardinality());
        }
    }

    @Test
    void shouldReturnFalseWhenBucketIsNotRemoved() {
        // Behavior: isRemoved returns false for a newly created bucket.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertFalse(BucketMetadataUtil.isRemoved(tr, metadata.subspace()));
        }
    }

    @Test
    void shouldSetRemovedAndReturnTrue() {
        // Behavior: setRemoved marks the bucket as removed, increments version, and publishes a journal event.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));
        long initialVersion = metadata.version();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(BucketMetadataUtil.isRemoved(tr, metadata.subspace()));

            // Verify that setRemoved increases the version
            long newVersion = BucketMetadataUtil.readVersion(tr, metadata.subspace());
            assertEquals(initialVersion + 1, newVersion);
        }

        // Verify that setRemoved publishes BucketMetadataUpdatedEvent
        ConsumerConfig cfg = new ConsumerConfig(
                UUID.randomUUID().toString(),
                JournalName.BUCKET_EVENTS.getValue(),
                ConsumerConfig.Offset.RESUME
        );
        Consumer consumer = new Consumer(context, cfg);
        try {
            consumer.start();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Event event = consumer.consume(tr);
                BucketMetadataUpdatedEvent evt = JSONUtil.readValue(event.value(), BucketMetadataUpdatedEvent.class);
                assertEquals(TEST_NAMESPACE, evt.namespace());
                assertEquals(TEST_BUCKET, evt.bucket());
                assertEquals(metadata.uuid(), evt.id());
            }
        } finally {
            consumer.stop();
        }
    }

    @Test
    void shouldOpenRemovedBucketWithRemovedFlagTrue() {
        // Behavior: forceOpen returns metadata with removed=true for a bucket that was marked as removed.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        // Mark the bucket as removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        // Flush the cache
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(context::now, 0);
        cleanup.run();

        // Open the removed bucket using forceOpen() and verify removed flag
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata openedMetadata = BucketMetadataUtil.forceOpen(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertTrue(openedMetadata.removed());
        }
    }

    @Test
    void shouldCreateBucketMetadata() {
        // Behavior: Transactional create initializes all metadata fields: name, namespace, version, id,
        // prefix, primary index, and removed == false.
        Session session = getSession();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.create(context, tr, session, TEST_BUCKET, List.of(TEST_SHARD_ID), null);
            tr.commit().join();

            assertEquals(TEST_BUCKET, metadata.name());
            assertEquals(TEST_NAMESPACE, metadata.namespace());
            assertTrue(metadata.version() > 0);
            assertNotNull(metadata.uuid());
            assertNotNull(metadata.prefix());
            assertNotNull(metadata.subspace());
            assertFalse(metadata.removed());

            Index index = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.ALL);
            assertNotNull(index);
            assertNotNull(index.subspace());
        }
    }

    @Test
    void shouldThrowOnEmptyShardsList() {
        // Behavior: create rejects an empty shards list with KronotopException.
        Session session = getSession();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopException exception = assertThrows(KronotopException.class, () ->
                    BucketMetadataUtil.create(context, tr, session, TEST_BUCKET, List.of(), null));
            assertEquals("Shards cannot be empty or null", exception.getMessage());
        }
    }

    @Test
    void shouldThrowBucketAlreadyExistsOnDuplicateCreate() {
        // Behavior: Calling create twice for the same bucket throws BucketAlreadyExistsException
        // because the volume prefix already exists.
        Session session = getSession();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadataUtil.create(context, tr, session, TEST_BUCKET, List.of(TEST_SHARD_ID), null);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThrows(BucketAlreadyExistsException.class, () ->
                    BucketMetadataUtil.create(context, tr, session, TEST_BUCKET, List.of(TEST_SHARD_ID), null));
        }
    }

    @Test
    void shouldThrowBucketAlreadyExistsOnRemovedBucket() {
        // Behavior: Creating a bucket that was previously created and then marked as removed still
        // throws BucketAlreadyExistsException because the volume prefix persists.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThrows(BucketAlreadyExistsException.class, () ->
                    BucketMetadataUtil.create(context, tr, session, TEST_BUCKET, List.of(TEST_SHARD_ID), null));
        }
    }

    @Test
    void shouldCreateAndCacheBucketMetadata() {
        // Behavior: The wrapper create(context, session, bucket) creates the bucket and populates
        // the metadata cache so that a subsequent cache lookup returns matching metadata.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        assertNotNull(metadata);
        assertEquals(TEST_BUCKET, metadata.name());

        BucketMetadata cached = context.getBucketMetadataCache().get(TEST_NAMESPACE, TEST_BUCKET);
        assertNotNull(cached);
        assertEquals(TEST_BUCKET, cached.name());
        assertEquals(metadata.uuid(), cached.uuid());
    }

    @Test
    void shouldPurgeBucket() {
        // Behavior: purge removes the bucket directory so that a subsequent open throws NoSuchBucketException.
        Session session = getSession();
        BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        // Purge the bucket
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.purge(tx, TEST_NAMESPACE, TEST_BUCKET);
            tr.commit().join();
        }

        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        // Verify bucket no longer exists after invalidating the cache.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NoSuchBucketException exception = assertThrows(NoSuchBucketException.class, () ->
                    BucketMetadataUtil.open(context, tr, session, TEST_BUCKET));
            assertEquals("No such bucket: 'test-bucket'", exception.getMessage());
        }
    }

    @Test
    void shouldSetAndReadShards() {
        // Behavior: setShards persists shard IDs sorted, increments the version, and round-trips through reload.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));
        long initialVersion = metadata.version();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setShards(tx, metadata, List.of(2, 0, 1));
            tr.commit().join();
        }

        // Flush cache and reload
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(context::now, 0);
        cleanup.run();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata reloaded = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertEquals(List.of(0, 1, 2), reloaded.shards());
            assertEquals(initialVersion + 1, reloaded.version());
        }
    }

    @Test
    void shouldFailWhenExpandingToMultiShardWithVectorIndex() {
        // Behavior: setShards rejects expanding to multiple shards when the bucket has a vector index.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        // Add a vector index to the bucket
        String vectorName = VectorIndexNameGenerator.generate("embedding", 3, DistanceFunction.COSINE);
        VectorIndexDefinition vectorDef = VectorIndexDefinition.create(vectorName, "embedding", 3, DistanceFunction.COSINE, IndexStatus.WAITING);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, metadata, vectorDef);
            tr.commit().join();
        }

        // Reload metadata so the vectorIndexes registry is populated
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }

        // Try to expand to multiple shards — should fail
        BucketMetadata finalMetadata = metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            KronotopException exception = assertThrows(KronotopException.class, () ->
                    BucketMetadataUtil.setShards(tx, finalMetadata, List.of(1, 2)));
            assertEquals("Vector indexes require single-shard buckets", exception.getMessage());
        }
    }

    @Test
    void shouldResolvePointerBytes() {
        // Behavior: resolvePointerBytes returns the UUID pointer bytes for a given prefix.
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] pointerBytes = BucketMetadataUtil.resolvePointerBytes(context, tr, metadata.prefix().asBytes());
            assertNotNull(pointerBytes);
            UUID resolved = UUIDUtil.fromBytes(pointerBytes);
            assertEquals(metadata.uuid(), resolved);
        }
    }

    @Test
    void shouldOpenByPrefix() {
        // Behavior: openByPrefix resolves bucket metadata from a volume prefix, returning metadata matching the original bucket.
        Session session = getSession();
        BucketMetadata expected = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.openByPrefix(context, tr, expected.prefix().asBytes());
            assertEquals(expected.name(), metadata.name());
            assertEquals(expected.namespace(), metadata.namespace());
            assertEquals(expected.uuid(), metadata.uuid());
            assertEquals(expected.prefix(), metadata.prefix());
            assertEquals(expected.shards(), metadata.shards());
            assertFalse(metadata.removed());
        }
    }

    @Test
    void shouldOpenByPrefixWithNestedNamespace() {
        // Behavior: openByPrefix correctly resolves a dotted namespace path from the namespace binding.
        Session session = getSession();
        String namespace = "x.y.z";
        NamespaceUtil.create(context, namespace);
        session.attr(SessionAttributes.CURRENT_NAMESPACE).set(namespace);

        BucketMetadata expected = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.openByPrefix(context, tr, expected.prefix().asBytes());
            assertEquals(namespace, metadata.namespace());
            assertEquals(TEST_BUCKET, metadata.name());
            assertEquals(expected.uuid(), metadata.uuid());
        }
    }

    @Test
    void shouldThrowWhenOpenByPrefixWithUnregisteredPrefix() {
        // Behavior: openByPrefix throws KronotopException for an unregistered prefix.
        byte[] bogusPrefix = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopException exception = assertThrows(KronotopException.class, () ->
                    BucketMetadataUtil.openByPrefix(context, tr, bogusPrefix));
            assertEquals("Prefix not registered", exception.getMessage());
        }
    }

    @Test
    void shouldResolveNamespacePath() {
        // Behavior: Resolves the full dotted namespace path from a bucket's namespace binding by walking parent pointers
        Session session = getSession();
        String namespace = "a.b.c.d";
        NamespaceUtil.create(context, namespace);
        session.attr(SessionAttributes.CURRENT_NAMESPACE).set(namespace);

        BucketMetadata metadata = BucketMetadataUtil.create(context, session, TEST_BUCKET, List.of(TEST_SHARD_ID));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] namespaceBinding = tr.get(namespaceBindingKey(metadata.subspace())).join();
            List<String> subpath = new ArrayList<>();
            NamespaceUtil.resolveNamespacePath(tr, new Subspace(namespaceBinding), subpath);
            assertEquals(namespace, String.join(".", subpath.reversed()));
        }
    }
}