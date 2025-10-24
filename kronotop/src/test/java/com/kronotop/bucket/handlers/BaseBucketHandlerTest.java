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

import com.kronotop.BaseHandlerTest;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketShard;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.protocol.CommitArgs;
import com.kronotop.protocol.CommitKeyword;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

public class BaseBucketHandlerTest extends BaseHandlerTest {
    protected final int SHARD_ID = 1;
    protected final byte[] DOCUMENT = BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}");

    protected List<byte[]> makeDummyDocument(int number) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            String document = String.format("{\"key\": \"value-%s\"}", i);
            result.add(BSONUtil.jsonToDocumentThenBytes(document));
        }
        return result;
    }

    protected void switchProtocol(BucketCommandBuilder<?, ?> cmd, RESPVersion version) {
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(version.getValue()).encode(buf);
        runCommand(channel, buf); // consume the response
    }

    protected byte[][] makeDocumentsArray(List<byte[]> documents) {
        byte[][] result = new byte[documents.size()][];
        for (int i = 0; i < documents.size(); i++) {
            byte[] document = documents.get(i);
            result[i] = document;
        }
        documents.toArray(result);
        return result;
    }

    protected Map<String, byte[]> insertDocuments(List<byte[]> documents) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(documents);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(documents.size(), actualMessage.children().size());
        Map<String, byte[]> result = new LinkedHashMap<>();
        for (int index = 0; index < actualMessage.children().size(); index++) {
            SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().get(index);
            assertNotNull(message.content());
            result.put(message.content(), documents.get(index));
        }
        return result;
    }

    /**
     * Inserts multiple documents into the specified bucket using a single transaction.
     * <p>
     * This method performs the following steps:
     * 1. Begins a new transaction.
     * 2. Inserts the provided documents into the bucket.
     * 3. Commits the transaction and retrieves the resulting document identifiers.
     *
     * @param bucket    The name of the bucket where the documents will be inserted.
     * @param documents A list of documents represented as byte arrays to be inserted into the bucket.
     * @return A list of document identifiers corresponding to the inserted documents.
     */
    protected List<String> insertManyDocumentsWithSingleTransaction(String bucket, List<byte[]> documents) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            BucketCommandBuilder<byte[], byte[]> bucketCommandBuilder = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            switchProtocol(bucketCommandBuilder, RESPVersion.RESP3);

            ByteBuf buf = Unpooled.buffer();
            byte[][] docs = makeDocumentsArray(documents);
            bucketCommandBuilder.insert(bucket, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(documents.size(), actualMessage.children().size());
        }

        ByteBuf buf = Unpooled.buffer();
        cmd.commit(CommitArgs.Builder.returning(CommitKeyword.FUTURES)).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
        assertEquals(1, actualMessage.children().size());

        List<String> ids = new ArrayList<>();
        MapRedisMessage result = (MapRedisMessage) actualMessage.children().getFirst();
        for (Map.Entry<RedisMessage, RedisMessage> entry : result.children().entrySet()) {
            ids.add(((SimpleStringRedisMessage) entry.getValue()).content());
        }
        return ids;
    }

    RedisMessage findInMapMessage(MapRedisMessage mapRedisMessage, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            if (keyMessage.content().equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    MapRedisMessage extractEntriesMap(Object response) {
        assertInstanceOf(MapRedisMessage.class, response);
        RedisMessage msg = findInMapMessage((MapRedisMessage) response, "entries");
        assertInstanceOf(MapRedisMessage.class, msg);
        return (MapRedisMessage) msg;
    }


    protected void insertAtBackground(CountDownLatch halfLatch, CountDownLatch allLatch, int totalInserts) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 32}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": 40}")
                ));

        for (int j = 0; j < totalInserts; j++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);
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
    }

    protected BucketShard getShard(int shardId) {
        return ((BucketService) context.getService(BucketService.NAME)).getShard(SHARD_ID);
    }
}
