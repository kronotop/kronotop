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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BackgroundIndexBuildingRoutineTest extends BaseBucketHandlerTest {
    @Test
    void shouldBuildIndexAtBackground() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 32}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 40}")
                ));
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<Versionstamp> expectedVersionstamps = new ArrayList<>();
        assertEquals(2, actualMessage.children().size());
        for (int i = 0; i < actualMessage.children().size(); i++) {
            SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().get(i);
            assertNotNull(message.content());
            expectedVersionstamps.add(VersionstampUtil.base32HexDecode(message.content()));
        }

        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "age",
                BsonType.INT32,
                IndexStatus.WAITING
        );

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
            IndexUtil.create(tr, metadata.subspace(), definition);
            tr.commit().join();
        }

        IndexBuildingTask task = new IndexBuildingTask(TEST_NAMESPACE, TEST_BUCKET, definition.id());
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            List<Long> expectedIndexValues = new ArrayList<>(List.of(32L, 40L));
            List<Long> indexValues = new ArrayList<>();
            List<Versionstamp> versionstamps = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                byte[] end = ByteArrayUtil.strinc(begin);
                for (KeyValue entry : tr.getRange(begin, end)) {
                    Tuple unpacked = index.subspace().unpack(entry.getKey());
                    indexValues.add(unpacked.getLong(2));
                    versionstamps.add((Versionstamp) unpacked.get(1));
                }
                return expectedVersionstamps.equals(versionstamps)
                        && expectedIndexValues.equals(indexValues);
            }
        });

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
                return IndexTaskStatus.COMPLETED == state.status(); // ready to check the IndexBuildingTaskState
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(state.cursorVersionstamp(), state.highestVersionstamp());
            assertNull(state.error());
            assertEquals(IndexTaskStatus.COMPLETED, state.status());
        }
    }
}