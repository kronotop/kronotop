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
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class BucketLocateHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    public void createTestBucket() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldLocateBucketWithSingleShard() {
        // Behavior: LOCATE on a bucket with one shard returns a 3-element array: [shardId, primaryAddress, emptyStandbysArray].
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.locate(TEST_BUCKET).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(3, array.children().size());

        // shard id
        assertInstanceOf(IntegerRedisMessage.class, array.children().get(0));
        IntegerRedisMessage shardIdMsg = (IntegerRedisMessage) array.children().get(0);
        assertEquals(TEST_SHARD_ID, shardIdMsg.value());

        // primary address (host:port)
        assertInstanceOf(FullBulkStringRedisMessage.class, array.children().get(1));
        FullBulkStringRedisMessage addressMsg = (FullBulkStringRedisMessage) array.children().get(1);
        String address = addressMsg.content().toString(StandardCharsets.US_ASCII);
        assertFalse(address.isEmpty());
        assertTrue(address.contains(":"));

        // standbys (empty in single-node)
        assertInstanceOf(ArrayRedisMessage.class, array.children().get(2));
        ArrayRedisMessage standbys = (ArrayRedisMessage) array.children().get(2);
        assertEquals(0, standbys.children().size());
    }

    @Test
    void shouldLocateBucketWithMultipleShards() {
        // Behavior: LOCATE on a bucket with multiple shards returns 3 elements per shard.
        String bucketName = "multi-shard-bucket";
        createBucket(bucketName, List.of(0, 1), null);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.locate(bucketName).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(6, array.children().size());

        Set<Long> shardIds = new HashSet<>();
        for (int i = 0; i < array.children().size(); i += 3) {
            assertInstanceOf(IntegerRedisMessage.class, array.children().get(i));
            shardIds.add(((IntegerRedisMessage) array.children().get(i)).value());

            assertInstanceOf(FullBulkStringRedisMessage.class, array.children().get(i + 1));
            String address = ((FullBulkStringRedisMessage) array.children().get(i + 1)).content().toString(StandardCharsets.US_ASCII);
            assertFalse(address.isEmpty());
            assertTrue(address.contains(":"));

            assertInstanceOf(ArrayRedisMessage.class, array.children().get(i + 2));
            ArrayRedisMessage standbys = (ArrayRedisMessage) array.children().get(i + 2);
            assertEquals(0, standbys.children().size());
        }

        assertTrue(shardIds.contains(0L));
        assertTrue(shardIds.contains(1L));
    }

    @Test
    void shouldReturnErrorForNonExistentBucket() {
        // Behavior: LOCATE on a bucket that doesn't exist returns an error.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.locate("non-existent-bucket").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) response;
        assertTrue(errorMessage.content().contains("No such bucket: 'non-existent-bucket'"));
    }
}
