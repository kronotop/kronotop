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

package com.kronotop.benchmark.bucket;

import com.kronotop.benchmark.BenchmarkRunner;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.SessionAttributeKeywords;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.bson.*;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

abstract class AbstractBucketBenchmark {
    protected static final String[] CATEGORIES = {"electronics", "clothing", "books", "sports", "food"};
    protected static final BsonDocumentCodec BSON_CODEC = new BsonDocumentCodec();

    protected static BsonDocument sampleDocument() {
        return generateDocument(new Random(42));
    }

    private static BsonDocument generateDocument(Random rng) {
        BsonDocument doc = new BsonDocument();
        doc.put("name", new BsonString(randomAlphanumeric(rng, 8 + rng.nextInt(5))));
        doc.put("age", new BsonInt32(18 + rng.nextInt(63)));
        doc.put("score", new BsonDouble(Math.round(rng.nextDouble() * 10000.0) / 100.0));
        doc.put("category", new BsonString(CATEGORIES[rng.nextInt(CATEGORIES.length)]));
        doc.put("active", new BsonBoolean(rng.nextBoolean()));
        BsonArray tags = new BsonArray();
        tags.add(new BsonString(randomAlphanumeric(rng, 4 + rng.nextInt(3))));
        tags.add(new BsonString(randomAlphanumeric(rng, 4 + rng.nextInt(3))));
        doc.put("tags", tags);
        return doc;
    }

    protected static byte[] serializeBson(BsonDocument doc) {
        BasicOutputBuffer buf = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(buf)) {
            BSON_CODEC.encode(writer, doc, EncoderContext.builder().build());
        }
        return buf.toByteArray();
    }

    protected static String randomAlphanumeric(Random rng, int length) {
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(rng.nextInt(chars.length())));
        }
        return sb.toString();
    }

    protected static List<List<byte[]>> partitionIntoBatches(List<byte[]> pool, int batchSize) {
        List<List<byte[]>> batches = new ArrayList<>();
        for (int i = 0; i < pool.size(); i += batchSize) {
            batches.add(pool.subList(i, Math.min(i + batchSize, pool.size())));
        }
        return batches;
    }

    protected void setupWorkerSession(StatefulRedisConnection<byte[], byte[]> conn) throws Exception {
        KronotopCommandBuilder<byte[], byte[]> k = new KronotopCommandBuilder<>(ByteArrayCodec.INSTANCE);
        BenchmarkRunner.dispatch(conn, k.sessionAttributeSet(SessionAttributeKeywords.INPUT_TYPE,
                "BSON".getBytes(StandardCharsets.UTF_8)));
        BenchmarkRunner.dispatch(conn, k.sessionAttributeSet(SessionAttributeKeywords.REPLY_TYPE,
                "BSON".getBytes(StandardCharsets.UTF_8)));
        BenchmarkRunner.dispatch(conn, k.sessionAttributeSet(SessionAttributeKeywords.OBJECT_ID_FORMAT,
                "BYTES".getBytes(StandardCharsets.UTF_8)));
    }

    protected List<byte[]> generatePool(int count) {
        Random rng = new Random(42);
        List<byte[]> pool = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            pool.add(serializeBson(generateDocument(rng)));
        }
        return pool;
    }

    protected void bulkInsert(List<List<byte[]>> batches, String bucketName, String host, int port) throws Exception {
        int total = batches.stream().mapToInt(List::size).sum();
        RedisClient loadClient = RedisClient.create(String.format("redis://%s:%d", host, port));
        try (StatefulRedisConnection<byte[], byte[]> conn = loadClient.connect(ByteArrayCodec.INSTANCE)) {
            BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            setupWorkerSession(conn);

            long loadStart = System.nanoTime();
            int inserted = 0;
            for (List<byte[]> batch : batches) {
                BenchmarkRunner.dispatch(conn, cmd.insert(bucketName, batch));
                inserted += batch.size();
                if (inserted % 5000 == 0 || inserted == total) {
                    System.out.printf("  Inserted %,d / %,d docs%n", inserted, total);
                }
            }
            double secs = (System.nanoTime() - loadStart) / 1_000_000_000.0;
            System.out.printf("Load complete: %,d docs in %.1f sec (%.0f docs/sec)%n",
                    total, secs, total / secs);
        } finally {
            loadClient.shutdown();
        }
    }
}
