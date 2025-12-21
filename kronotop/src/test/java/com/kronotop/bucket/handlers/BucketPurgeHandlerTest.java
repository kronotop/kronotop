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

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.DataStructureKind;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.NoSuchBucketException;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.worker.Worker;
import com.kronotop.worker.WorkerTag;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketPurgeHandlerTest extends BaseBucketHandlerTest {

    @Test
    void shouldPurgeBucketSuccessfully() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Insert a document to create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);

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
            NoSuchBucketException exception = assertThrows(NoSuchBucketException.class, () -> {
                BucketMetadataUtil.forceOpen(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            });
            assertEquals("No such bucket: 'test-bucket'", exception.getMessage());
        }
    }

    @Test
    void shouldThrowErrorWhenPurgingNonExistentBucket() {
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
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Insert a document to create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);

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
            cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);

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
            NoSuchBucketException exception = assertThrows(NoSuchBucketException.class, () -> {
                BucketMetadataUtil.forceOpen(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            });
            assertEquals("No such bucket: 'test-bucket'", exception.getMessage());
        }
    }

    @Test
    void shouldFailPurgeWhenWorkerDoesNotRemoveItself() {
        class StubbornWorker implements Worker {
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
                // Does NOT remove itself from registry (simulating a stuck worker)
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
            cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // Create and register a stubborn worker that won't remove itself
        StubbornWorker worker = new StubbornWorker();
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

        // Wait for shutdown to be called, but worker stays in registry
        await().atMost(1, TimeUnit.SECONDS).until(worker::isShutdownCalled);

        // Worker should still be in registry
        List<Worker> remaining = context.getWorkerRegistry().get(TEST_NAMESPACE, tag);
        assertEquals(1, remaining.size());

        {
            // Purge bucket - should fail because barrier is not satisfied
            ByteBuf buf = Unpooled.buffer();
            cmd.purge(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertTrue(actualMessage.content().startsWith("ERR Barrier not satisfied"));
        }

        // Cleanup: remove worker from registry to allow test teardown
        context.getWorkerRegistry().remove(TEST_NAMESPACE, worker);
    }
}