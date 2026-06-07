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

import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketListHandlerTest extends BaseBucketHandlerTest {

    @Test
    void shouldReturnEmptyArrayWhenNoBucketsExist() {
        // Behavior: BUCKET.LIST on a fresh namespace returns an empty array.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.list().encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertTrue(array.children().isEmpty());
    }

    @Test
    void shouldListSingleBucket() {
        // Behavior: BUCKET.LIST returns an array containing the single created bucket name.
        createBucket(TEST_BUCKET);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.list().encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());

        FullBulkStringRedisMessage nameMsg = (FullBulkStringRedisMessage) array.children().getFirst();
        assertEquals(TEST_BUCKET, nameMsg.content().toString(StandardCharsets.UTF_8));
    }

    @Test
    void shouldListMultipleBuckets() {
        // Behavior: BUCKET.LIST returns all created bucket names.
        List<String> bucketNames = List.of("alpha-bucket", "beta-bucket", "gamma-bucket");
        for (String name : bucketNames) {
            createBucket(name);
        }

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.list().encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(bucketNames.size(), array.children().size());

        List<String> actual = new ArrayList<>();
        for (var child : array.children()) {
            assertInstanceOf(FullBulkStringRedisMessage.class, child);
            actual.add(((FullBulkStringRedisMessage) child).content().toString(StandardCharsets.UTF_8));
        }
        assertTrue(actual.containsAll(bucketNames));
    }

    @Test
    void shouldNotListPurgedBuckets() {
        // Behavior: A bucket that has been removed and then purged no longer appears in BUCKET.LIST output.
        // Removed-but-not-purged buckets are intentionally still listed.
        createBucket(TEST_BUCKET);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        {
            // Remove the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            // Purge the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.purge(TEST_BUCKET).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        });

        {
            // List should be empty now
            ByteBuf buf = Unpooled.buffer();
            cmd.list().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage array = (ArrayRedisMessage) response;
            assertTrue(array.children().isEmpty());
        }
    }
}
