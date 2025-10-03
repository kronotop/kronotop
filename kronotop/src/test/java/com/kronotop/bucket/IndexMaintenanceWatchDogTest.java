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

package com.kronotop.bucket;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.awaitility.Awaitility.await;

class IndexMaintenanceWatchDogTest extends BaseBucketHandlerTest {
    private final Lock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();
    private volatile boolean condition = false;

    @BeforeAll
    static void setUp() {
        System.setProperty("__test__.background_index_builder.skip_wait_transaction_limit", "true");
    }

    @BeforeAll
    static void teardown() {
        System.setProperty("__test__.background_index_builder.skip_wait_transaction_limit", "false");
    }

    @Test
    void shouldBuildIndexAtBackground2() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(500);
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.submit(() -> background(latch));

        latch.await();
        System.out.println("latch bitti");

        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "age",
                BsonType.INT32,
                IndexStatus.WAITING
        );

        DirectorySubspace taskSubspace = IndexTaskUtil.createOrOpenTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);
            IndexUtil.create(tr, metadata.subspace(), definition);
            tr.commit().join();
        }

        IndexBuilderTask task = new IndexBuilderTask(NAMESPACE_NAME, BUCKET_NAME, definition.id());
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));
        context.getFoundationDB().run(tr -> {
            IndexBuilderTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.WAITING);
            return null;
        });

        System.out.println("index olusturma bitti");

        lock.lock();
        try {
            while (!condition) {
                cond.await();
            }
        } finally {
            lock.unlock();
        }

        System.out.println("insert bitti");

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            List<Versionstamp> versionstamps = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, NAMESPACE_NAME, BUCKET_NAME);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    versionstamps.add((Versionstamp) unpacked.get(1));
                }
            }
            System.out.println(versionstamps.size());
            return versionstamps.size() == 2000;
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilderTaskState state = IndexBuilderTaskState.load(tr, taskSubspace, taskId);
            System.out.println(state.cursorVersionstamp());
            System.out.println(state.highestVersionstamp());
            System.out.println(state.error());
            System.out.println(state.status());
        }
    }

    private void background(CountDownLatch latch) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 32}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 40}")
                ));

        for (int j = 0; j < 1000; j++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(BUCKET_NAME, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

            runCommand(channel, buf);

            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
        }

        // uyandıran thread
        lock.lock();
        try {
            condition = true;
            cond.signal();
        } finally {
            lock.unlock();
        }
    }
}