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

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.protocol.CommitArgs;
import com.kronotop.protocol.CommitKeyword;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketInsertHandlerTest extends BaseBucketHandlerTest {

    @Test
    void test_insert_single_document_with_oneOff_transaction() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(BUCKET_NAME, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(1, actualMessage.children().size());
        SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().getFirst();
        assertNotNull(message.content());

        Versionstamp versionstamp = assertDoesNotThrow(() -> VersionstampUtil.base32HexDecode(message.content()));
        assertEquals(0, versionstamp.getUserVersion());
    }

    @Test
    void test_insert_documents_with_oneOff_transaction() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"three\": \"four\"}")
                ));
        cmd.insert(BUCKET_NAME, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(2, actualMessage.children().size());
        for (int i = 0; i < actualMessage.children().size(); i++) {
            SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().get(i);
            assertNotNull(message.content());
        }
    }

    @Test
    void test_insert_within_transaction() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        // BEGIN
        {
            ByteBuf buf = Unpooled.buffer();
            // Create a new transaction
            cmd.begin().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // BUCKET.INSERT <bucket-name> <document> <document>
        {
            ByteBuf buf = Unpooled.buffer();
            BucketCommandBuilder<byte[], byte[]> bucketCommandBuilder = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            byte[][] docs = makeDocumentsArray(
                    List.of(
                            BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}"),
                            BSONUtil.jsonToDocumentThenBytes("{\"three\": \"four\"}")
                    ));
            bucketCommandBuilder.insert(BUCKET_NAME, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);

            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(2, actualMessage.children().size());

            // User versions have returned
            for (RedisMessage redisMessage : actualMessage.children()) {
                assertInstanceOf(IntegerRedisMessage.class, redisMessage);
            }
        }

        // COMMIT
        {
            // Commit the changes
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    void test_insert_commit_with_futures() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        // BEGIN
        {
            ByteBuf buf = Unpooled.buffer();
            // Create a new transaction
            cmd.begin().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // BUCKET.INSERT <bucket-name> <document> <document>
        {
            ByteBuf buf = Unpooled.buffer();
            BucketCommandBuilder<byte[], byte[]> bucketCommandBuilder = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            byte[][] docs = makeDocumentsArray(
                    List.of(
                            BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}"),
                            BSONUtil.jsonToDocumentThenBytes("{\"three\": \"four\"}")
                    ));
            bucketCommandBuilder.insert(BUCKET_NAME, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);

            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(2, actualMessage.children().size());

            // User versions have returned
            for (RedisMessage redisMessage : actualMessage.children()) {
                assertInstanceOf(IntegerRedisMessage.class, redisMessage);
            }
        }

        // COMMIT
        {
            // Commit the changes
            ByteBuf buf = Unpooled.buffer();
            cmd.commit(CommitArgs.Builder.returning(CommitKeyword.FUTURES)).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);

            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(1, actualMessage.children().size());
            RedisMessage result = actualMessage.children().getFirst();
            assertInstanceOf(MapRedisMessage.class, result);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) result;

            for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
                assertInstanceOf(IntegerRedisMessage.class, entry.getKey());
                IntegerRedisMessage userVersion = (IntegerRedisMessage) entry.getKey();

                assertInstanceOf(SimpleStringRedisMessage.class, entry.getValue());
                SimpleStringRedisMessage id = (SimpleStringRedisMessage) entry.getValue();

                Versionstamp decodedId = VersionstampUtil.base32HexDecode(id.content());
                assertEquals(decodedId.getUserVersion(), userVersion.value());
            }
        }
    }
}
