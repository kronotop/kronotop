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

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class InfoHandlerBucketTest extends BaseBucketHandlerTest {

    private String runInfo(String section) {
        String raw = "*2\r\n$4\r\nINFO\r\n$" + section.length() + "\r\n" + section + "\r\n";
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(raw.getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, response);
        return ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.US_ASCII);
    }

    private static String fieldValue(String info, String key) {
        for (String line : info.split("\r\n")) {
            if (line.startsWith(key + ":")) {
                return line.substring(key.length() + 1);
            }
        }
        return null;
    }

    private static long attribute(String line, String key) {
        for (String pair : line.split(",")) {
            if (pair.startsWith(key + "=")) {
                return Long.parseLong(pair.substring(key.length() + 1));
            }
        }
        throw new AssertionError("Attribute not found: " + key);
    }

    private long bucketVolumeAppends() {
        String info = runInfo("volume");
        int count = Integer.parseInt(Objects.requireNonNull(fieldValue(info, "volume_count")));
        long appends = 0;
        for (int i = 0; i < count; i++) {
            String line = fieldValue(info, "volume" + i);
            assertNotNull(line);
            if (line.startsWith("name=bucket-shard-")) {
                appends += attribute(line, "appends");
            }
        }
        return appends;
    }

    @Test
    void shouldGrowPlanCacheAfterQuery() {
        // Behavior: running a query stores its plan, so plan_cache_size grows by one
        createBucket(TEST_BUCKET);
        insertRaw("{\"name\": \"alice\"}");
        int before = Integer.parseInt(Objects.requireNonNull(fieldValue(runInfo("bucket"), "plan_cache_size")));

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": \"alice\"}").encode(buf);
        runCommand(channel, buf);

        int after = Integer.parseInt(Objects.requireNonNull(fieldValue(runInfo("bucket"), "plan_cache_size")));
        assertEquals(before + 1, after);
    }

    @Test
    void shouldCountVolumeAppends() {
        // Behavior: an insert is one append on the bucket shard volume that stores the document
        createBucket(TEST_BUCKET);
        long before = bucketVolumeAppends();

        insertRaw("{\"name\": \"alice\"}");

        long after = bucketVolumeAppends();
        assertEquals(before + 1, after);
    }

    @Test
    void shouldCountVectorIndexes() {
        // Behavior: a bucket with a vector index and one inserted document opens an on-heap
        // graph index, so vector_indexes is at least one
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes(
                "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"
        )).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("alpha"));
        BsonArray embedding = new BsonArray();
        embedding.add(new BsonDouble(0.1));
        embedding.add(new BsonDouble(0.2));
        embedding.add(new BsonDouble(0.3));
        doc.put("embedding", embedding);
        buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.toBytes(doc)).encode(buf);
        assertInstanceOf(ArrayRedisMessage.class, runCommand(channel, buf));

        buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f}).encode(buf);
        assertInstanceOf(ArrayRedisMessage.class, runCommand(channel, buf));

        String info = runInfo("vector");
        assertTrue(Integer.parseInt(Objects.requireNonNull(fieldValue(info, "vector_indexes"))) >= 1);
        assertTrue(Long.parseLong(Objects.requireNonNull(fieldValue(info, "vector_bytes_used"))) >= 0);
    }
}
