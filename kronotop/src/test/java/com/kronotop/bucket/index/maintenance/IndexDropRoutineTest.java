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
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.server.resp3.ArrayRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class IndexDropRoutineTest extends BaseBucketHandlerTest {
    @Test
    void shouldDropIndexInBackground() {
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
        assertEquals(2, actualMessage.children().size());

        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "age",
                BsonType.INT32,
                IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        Index index = metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        assertNotNull(index);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.drop(tx, metadata, definition.name());
            tr.commit().join();
        }

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                try {
                    IndexUtil.open(tr, metadata.subspace(), definition.name());
                    return false;
                } catch (NoSuchIndexException exp) {
                    // Dropped
                    return true;
                }
            }
        });
    }
}