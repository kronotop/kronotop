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

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseHandlerTest;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexUtil;
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

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

public class BaseBucketHandlerTest extends BaseHandlerTest {
    protected final int SHARD_ID = 1;
    protected final byte[] DOCUMENT = BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}");
    protected final Random rand = new Random(System.nanoTime());

    /**
     * Creates a list of dummy documents with sequential key-value pairs.
     *
     * @param number the number of documents to generate
     * @return list of BSON-encoded documents
     */
    protected List<byte[]> makeDummyDocument(int number) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            String document = String.format("{\"key\": \"value-%s\"}", i);
            result.add(BSONUtil.jsonToDocumentThenBytes(document));
        }
        return result;
    }

    /**
     * Switches the RESP protocol version for the test channel.
     *
     * @param cmd     the bucket command builder
     * @param version the target RESP protocol version
     */
    protected void switchProtocol(BucketCommandBuilder<?, ?> cmd, RESPVersion version) {
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(version.getValue()).encode(buf);
        runCommand(channel, buf); // consume the response
    }

    /**
     * Converts a list of documents to a byte array.
     *
     * @param documents the list of BSON documents
     * @return two-dimensional byte array
     */
    protected byte[][] makeDocumentsArray(List<byte[]> documents) {
        byte[][] result = new byte[documents.size()][];
        for (int i = 0; i < documents.size(); i++) {
            byte[] document = documents.get(i);
            result[i] = document;
        }
        documents.toArray(result);
        return result;
    }

    /**
     * Inserts documents into the test bucket and returns a map of document IDs to documents.
     *
     * @param documents the documents to insert
     * @return map of document IDs to their corresponding documents
     */
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
     * Inserts documents in batches and returns a map of all document IDs to documents.
     *
     * @param documents the documents to insert
     * @param batchSize the number of documents per batch
     * @return map of all document IDs to their corresponding documents
     */
    protected Map<String, byte[]> insertDocuments(List<byte[]> documents, int batchSize) {
        Map<String, byte[]> parent = new HashMap<>();
        int index = 0;
        while (index < documents.size()) {
            List<byte[]> subDocs = new ArrayList<>();
            for (int counter = 0; counter < batchSize; counter++) {
                byte[] document = documents.get(index);
                subDocs.add(document);
                index++;
            }
            Map<String, byte[]> child = insertDocuments(subDocs);
            parent.putAll(child);
        }
        return parent;
    }

    /**
     * Inserts documents within a single transaction and returns their IDs.
     *
     * @param bucket    the target bucket name
     * @param documents the documents to insert
     * @return list of generated document IDs
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

    /**
     * Finds a value in a MapRedisMessage by key.
     *
     * @param mapRedisMessage the map message to search
     * @param key             the key to find
     * @return the value message, or null if not found
     */
    RedisMessage findInMapMessage(MapRedisMessage mapRedisMessage, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            if (keyMessage.content().equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Extracts the "entries" map from a response message.
     *
     * @param response the response message
     * @return the entries map message
     */
    MapRedisMessage extractEntriesMap(Object response) {
        assertInstanceOf(MapRedisMessage.class, response);
        RedisMessage msg = findInMapMessage((MapRedisMessage) response, "entries");
        assertInstanceOf(MapRedisMessage.class, msg);
        return (MapRedisMessage) msg;
    }


    /**
     * Inserts documents in the background with latch coordination for concurrent testing.
     *
     * @param halfLatch    countdown latch for half completion
     * @param allLatch     countdown latch for full completion
     * @param totalInserts total number of insert operations to perform
     */
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

    /**
     * Retrieves a bucket shard by its ID.
     *
     * @param shardId the shard ID
     * @return the bucket shard
     */
    protected BucketShard getShard(int shardId) {
        return ((BucketService) context.getService(BucketService.NAME)).getShard(SHARD_ID);
    }

    /**
     * Generates documents with a specified field containing random numeric values.
     *
     * @param field  the field name
     * @param number the number of documents to generate
     * @return list of BSON-encoded documents
     */
    protected List<byte[]> generateRandomDocumentsWithNumericContent(String field, int number) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            String document = String.format("{\"%s\": %d}", field, rand.nextInt(0, Integer.MAX_VALUE));
            result.add(BSONUtil.jsonToDocumentThenBytes(document));
        }
        return result;
    }

    /**
     * Selects random keys from a map up to a specified count.
     *
     * @param items the map to select from
     * @param count the maximum number of keys to select
     * @return list of randomly selected keys
     */
    protected List<String> selectRandomKeysFromMap(Map<String, byte[]> items, int count) {
        List<String> allKeys = new ArrayList<>(items.keySet());
        Collections.shuffle(allKeys, new Random(System.nanoTime()));
        return allKeys.subList(0, Math.min(count, allKeys.size()));
    }

    /**
     * Loads an index definition by selector.
     *
     * @param selector the index selector
     * @return the index definition
     */
    protected IndexDefinition loadIndexDefinition(String selector) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.forceOpen(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index index = metadata.indexes().getIndex(selector, IndexSelectionPolicy.ALL);
            return IndexUtil.loadIndexDefinition(tr, index.subspace());
        }
    }
}
