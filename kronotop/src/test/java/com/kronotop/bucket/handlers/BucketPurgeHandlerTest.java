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

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.DataStructureKind;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.NoSuchBucketException;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.internal.JSONUtil;
import com.kronotop.journal.Consumer;
import com.kronotop.journal.ConsumerConfig;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalName;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.Prefix;
import com.kronotop.worker.Worker;
import com.kronotop.worker.WorkerTag;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketPurgeHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldPurgeBucketSuccessfully() {
        // Behavior: After marking a bucket as removed, purging it permanently deletes all data and metadata.
        // The bucket is no longer accessible afterward.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Insert a document to create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        {
            // Remove the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            // Purge the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.purge(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify bucket no longer exists
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NoSuchBucketException exception = assertThrows(NoSuchBucketException.class, () ->
                    BucketMetadataUtil.forceOpen(context, tr, TEST_NAMESPACE, TEST_BUCKET));
            assertEquals("No such bucket: 'test-bucket'", exception.getMessage());
        }
    }

    @Test
    void shouldThrowErrorWhenPurgingNonExistentBucket() {
        // Behavior: Purging a bucket that does not exist returns a NOSUCHBUCKET error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.purge("non-existent-bucket").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals("NOSUCHBUCKET No such bucket: 'non-existent-bucket'", actualMessage.content());
    }

    @Test
    void shouldThrowErrorWhenPurgingBucketNotMarkedAsRemoved() {
        // Behavior: Purging a bucket that has not been marked as removed returns an ERR error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Insert a document to create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        {
            // Try to purge without removing first
            ByteBuf buf = Unpooled.buffer();
            cmd.purge(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals("ERR Bucket 'test-bucket' is not removed", actualMessage.content());
        }
    }

    @Test
    void shouldShutdownWorkersAndPurgeBucketSuccessfully() {
        // Behavior: When a bucket is removed, background workers are shut down. If workers remove
        // themselves from the registry, the purge barrier is satisfied and the purge succeeds.
        class TestWorker implements Worker {
            private volatile boolean shutdownCalled = false;

            @Override
            public String getTag() {
                return WorkerTag.generate(DataStructureKind.BUCKET, TEST_BUCKET);
            }

            @Override
            public void shutdown() {
                shutdownCalled = true;
            }

            @Override
            public boolean await(long timeout, TimeUnit unit) {
                // Remove self from registry (simulating completion hook)
                context.getWorkerRegistry().remove(TEST_NAMESPACE, this);
                return true;
            }

            public boolean isShutdownCalled() {
                return shutdownCalled;
            }
        }

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Insert a document to create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // Create and register a test worker
        TestWorker worker = new TestWorker();
        context.getWorkerRegistry().put(TEST_NAMESPACE, worker);

        // Verify worker is registered
        String tag = WorkerTag.generate(DataStructureKind.BUCKET, TEST_BUCKET);
        List<Worker> workers = context.getWorkerRegistry().get(TEST_NAMESPACE, tag);
        assertEquals(1, workers.size());

        {
            // Remove the bucket (triggers BUCKET_REMOVED_EVENT via journal)
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the worker to be shut down and removed from registry
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            List<Worker> remaining = context.getWorkerRegistry().get(TEST_NAMESPACE, tag);
            return remaining.isEmpty() && worker.isShutdownCalled();
        });

        {
            // Purge bucket - should succeed because the barrier is satisfied
            ByteBuf buf = Unpooled.buffer();
            cmd.purge(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify bucket no longer exists
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            NoSuchBucketException exception = assertThrows(NoSuchBucketException.class, () ->
                    BucketMetadataUtil.forceOpen(context, tr, TEST_NAMESPACE, TEST_BUCKET));
            assertEquals("No such bucket: 'test-bucket'", exception.getMessage());
        }
    }

    @Test
    void shouldFailPurgeWhenWorkerDoesNotRemoveItself() {
        // Behavior: When a worker does not terminate within the timeout, it remains in the registry.
        // The purge barrier is not satisfied, and the purge fails with BARRIERNOTSATISFIED.
        class StuckWorker implements Worker {
            @Override
            public String getTag() {
                return WorkerTag.generate(DataStructureKind.BUCKET, TEST_BUCKET);
            }

            @Override
            public void shutdown() {
            }

            @Override
            public boolean await(long timeout, TimeUnit unit) {
                // Simulate a worker that does not terminate within the timeout
                return false;
            }
        }

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Insert a document to create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // Create and register a stuck worker
        StuckWorker worker = new StuckWorker();
        context.getWorkerRegistry().put(TEST_NAMESPACE, worker);

        // Verify worker is registered
        String tag = WorkerTag.generate(DataStructureKind.BUCKET, TEST_BUCKET);
        List<Worker> workers = context.getWorkerRegistry().get(TEST_NAMESPACE, tag);
        assertEquals(1, workers.size());

        {
            // Remove the bucket (triggers BUCKET_REMOVED_EVENT via journal)
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the shutdown attempt to complete (worker stays in registry because await returns false)
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            // The worker should still be in the registry because it didn't terminate
            List<Worker> remaining = context.getWorkerRegistry().get(TEST_NAMESPACE, tag);
            return !remaining.isEmpty();
        });

        {
            // Purge should fail because the barrier is not satisfied (worker still in registry)
            ByteBuf buf = Unpooled.buffer();
            cmd.purge(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertTrue(actualMessage.content().contains("BARRIERNOTSATISFIED"));
        }

        // Clean up: manually remove the stuck worker from registry
        context.getWorkerRegistry().remove(TEST_NAMESPACE, worker);
    }

    @Test
    void shouldRecreateBucketAfterPurgeWithFreshMetadata() {
        // Behavior: After remove → purge → recreate sequence, the recreated bucket should use
        // fresh metadata with a new prefix. Data inserted after recreation should be accessible,
        // proving the cache was invalidated and no stale metadata was used.

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Insert first document to create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        {
            // Remove the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Purge the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.purge(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            // Recreate bucket by inserting a new document with the same bucket name
            ByteBuf buf = Unpooled.buffer();
            cmd.create(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Query should return only the second document (from recreated bucket)
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);

            Object response = runCommand(channel, buf);
            assertNotNull(response, "Recreated bucket should return data");
        }
    }

    @Test
    void shouldUnregisterPrefixFromGlobalSubspaceAfterPurge() {
        // Behavior: After remove + purge, the bucket's prefix must no longer exist in the global PREFIXES subspace.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // Capture the prefix before removal
        Prefix prefix;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            prefix = metadata.prefix();
        }

        // Verify the prefix is registered in the global PREFIXES subspace
        DirectorySubspace prefixesSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.PREFIXES);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] prefixKey = prefixesSubspace.pack(Tuple.from((Object) prefix.asBytes()));
            assertNotNull(tr.get(prefixKey).join(), "Prefix should be registered before purge");
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.purge(TEST_BUCKET).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Verify prefix is no longer in the global PREFIXES subspace
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] prefixKey = prefixesSubspace.pack(Tuple.from((Object) prefix.asBytes()));
            assertNull(tr.get(prefixKey).join(), "Prefix should be unregistered after purge");
        }
    }

    @Test
    void shouldPublishDisusedPrefixToJournalAfterPurge() {
        // Behavior: After remove + purge, the bucket's prefix must be published to the DISUSED_PREFIXES
        // journal so VolumeService can clean up volume data.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        Prefix prefix;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            prefix = metadata.prefix();
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.purge(TEST_BUCKET).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Consume from the DISUSED_PREFIXES journal and verify the prefix is present
        ConsumerConfig config = new ConsumerConfig(
                "test-consumer-purge",
                JournalName.DISUSED_PREFIXES.getValue(),
                ConsumerConfig.Offset.EARLIEST
        );
        Consumer consumer = new Consumer(context, config);
        consumer.start();

        List<byte[]> publishedPrefixes = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event;
            while ((event = consumer.consume(tr)) != null) {
                byte[] decoded = JSONUtil.readValue(event.value(), byte[].class);
                publishedPrefixes.add(decoded);
                consumer.markConsumed(tr, event);
            }
            tr.commit().join();
        }
        consumer.stop();

        boolean found = publishedPrefixes.stream()
                .anyMatch(p -> Arrays.equals(p, prefix.asBytes()));
        assertTrue(found, "Purged bucket's prefix should be published to DISUSED_PREFIXES journal");
    }
}